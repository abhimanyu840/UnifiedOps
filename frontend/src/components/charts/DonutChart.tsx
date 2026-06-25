import { useState, type ReactNode } from 'react';

export interface DonutSlice {
  name: string;
  value: number;
  color: string;
}

interface Props {
  data: DonutSlice[];
  size?: number;
  innerRadius?: number;
  outerRadius?: number;
  showPercentLabels?: boolean;
  centerContent?: ReactNode;
  onSliceClick?: (slice: DonutSlice) => void;
  className?: string;
}

const polar = (cx: number, cy: number, r: number, deg: number) => {
  const a = ((deg - 90) * Math.PI) / 180;
  return { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
};

const arcPath = (
  cx: number,
  cy: number,
  rOuter: number,
  rInner: number,
  start: number,
  end: number,
): string => {
  const sweep = end - start;

  // Full ring (single 100% slice). The standard arc path collapses to a
  // line because the start and end points are identical, so we draw it
  // explicitly as two 180° arcs for the outer edge and two for the
  // inner edge (outer clockwise, inner counter-clockwise) and let
  // fill-rule="evenodd" cut the hole.
  if (sweep >= 359.999) {
    const oL = { x: cx - rOuter, y: cy };
    const oR = { x: cx + rOuter, y: cy };
    const iL = { x: cx - rInner, y: cy };
    const iR = { x: cx + rInner, y: cy };
    return [
      `M ${oL.x} ${oL.y}`,
      `A ${rOuter} ${rOuter} 0 1 1 ${oR.x} ${oR.y}`,
      `A ${rOuter} ${rOuter} 0 1 1 ${oL.x} ${oL.y}`,
      'Z',
      `M ${iL.x} ${iL.y}`,
      `A ${rInner} ${rInner} 0 1 0 ${iR.x} ${iR.y}`,
      `A ${rInner} ${rInner} 0 1 0 ${iL.x} ${iL.y}`,
      'Z',
    ].join(' ');
  }

  const largeArc = sweep > 180 ? 1 : 0;
  const p1 = polar(cx, cy, rOuter, start);
  const p2 = polar(cx, cy, rOuter, end);
  const p3 = polar(cx, cy, rInner, end);
  const p4 = polar(cx, cy, rInner, start);
  return [
    `M ${p1.x} ${p1.y}`,
    `A ${rOuter} ${rOuter} 0 ${largeArc} 1 ${p2.x} ${p2.y}`,
    `L ${p3.x} ${p3.y}`,
    `A ${rInner} ${rInner} 0 ${largeArc} 0 ${p4.x} ${p4.y}`,
    'Z',
  ].join(' ');
};

export function DonutChart({
  data,
  size = 220,
  innerRadius,
  outerRadius,
  showPercentLabels = true,
  centerContent,
  onSliceClick,
  className,
}: Props) {
  const cx = size / 2;
  const cy = size / 2;
  const rOuter = outerRadius ?? size / 2 - 8;
  const rInner = innerRadius ?? size / 4;
  const activeBoost = 6;

  const [active, setActive] = useState<number | null>(null);

  const total = data.reduce((acc, d) => acc + d.value, 0);
  const nonZero = data.filter(d => d.value > 0);
  const placeholder = total === 0;
  const effective = placeholder
    ? data.map(d => ({ ...d, value: 1 }))
    : nonZero;
  const effTotal = effective.reduce((a, c) => a + c.value, 0) || 1;

  let cursor = 0;
  const slices = effective.map((d, i) => {
    const sweep = (d.value / effTotal) * 360;
    const start = cursor;
    const end = cursor + sweep;
    cursor = end;
    const isFullRing = sweep >= 359.999;
    // Anchor labels at the top of a full-ring slice so the count/pct don't
    // crowd the bottom of the donut.
    const mid = isFullRing ? 0 : start + sweep / 2;
    const labelR = (rInner + rOuter) / 2;
    const { x, y } = polar(cx, cy, labelR, mid);
    const pct = total > 0 ? (d.value / total) * 100 : 0;
    return { key: `${d.name}-${i}`, d, start, end, labelX: x, labelY: y, pct };
  });

  return (
    <svg
      width={size}
      height={size}
      viewBox={`0 0 ${size} ${size}`}
      className={className}
      role="img"
      style={{ overflow: 'visible' }}
    >
      <defs>
        {data.map(d => (
          <radialGradient
            key={`g-${d.name}`}
            id={`donut-grad-${d.name.replace(/[^a-zA-Z0-9_]/g, '_')}`}
            cx="50%"
            cy="50%"
            r="65%"
          >
            <stop offset="60%" stopColor={d.color} stopOpacity={1} />
            <stop offset="100%" stopColor={d.color} stopOpacity={0.7} />
          </radialGradient>
        ))}
      </defs>

      <circle cx={cx} cy={cy} r={rOuter + 1}
        fill="none" stroke="rgba(255,255,255,0.04)" strokeWidth={1} />

      {slices.map((s, i) => {
        const isActive = active === i && !placeholder;
        const ro = isActive ? rOuter + activeBoost : rOuter;
        return (
          <path
            key={s.key}
            d={arcPath(cx, cy, ro, rInner, s.start, s.end)}
            fill={`url(#donut-grad-${s.d.name.replace(/[^a-zA-Z0-9_]/g, '_')})`}
            fillOpacity={placeholder ? 0.15 : 1}
            stroke="#10141d"
            strokeWidth={2}
            style={{
              cursor: placeholder ? 'default' : onSliceClick ? 'pointer' : 'default',
              filter: isActive
                ? `drop-shadow(0 0 10px ${s.d.color}80)`
                : 'none',
              transition: 'd 120ms ease, filter 120ms ease',
            }}
            onMouseEnter={() => setActive(i)}
            onMouseLeave={() => setActive(prev => (prev === i ? null : prev))}
            onClick={() => !placeholder && onSliceClick?.(s.d)}
          />
        );
      })}

      {showPercentLabels && !placeholder && slices.map(s => {
        if (s.pct < 3) return null;
        const big = s.pct >= 10;
        return (
          <g key={`l-${s.key}`} style={{ pointerEvents: 'none' }}>
            <text
              x={s.labelX}
              y={s.labelY - (big ? 5 : 3)}
              textAnchor="middle"
              dominantBaseline="central"
              fill="#fff"
              fontSize={big ? 13 : 10}
              fontWeight={800}
              style={{
                paintOrder: 'stroke',
                stroke: 'rgba(0,0,0,0.45)',
                strokeWidth: 2.5,
              }}
            >
              {s.d.value}
            </text>
            <text
              x={s.labelX}
              y={s.labelY + (big ? 7 : 5)}
              textAnchor="middle"
              dominantBaseline="central"
              fill="rgba(255,255,255,0.92)"
              fontSize={big ? 9.5 : 8.5}
              fontWeight={600}
              style={{
                paintOrder: 'stroke',
                stroke: 'rgba(0,0,0,0.45)',
                strokeWidth: 2,
              }}
            >
              {`${s.pct.toFixed(0)}%`}
            </text>
          </g>
        );
      })}

      {centerContent && (
        <foreignObject
          x={cx - rInner}
          y={cy - rInner}
          width={rInner * 2}
          height={rInner * 2}
          style={{ pointerEvents: 'none' }}
        >
          <div
            style={{
              width: '100%',
              height: '100%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexDirection: 'column',
            }}
          >
            {centerContent}
          </div>
        </foreignObject>
      )}
    </svg>
  );
}
