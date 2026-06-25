import { useRef, useState } from 'react';
import type { SparkPoint } from '../../types';
import { ArrowUpIcon } from '../icons/Icons';
import { AreaChart, type AreaPoint } from '../charts/AreaChart';
import { Card, CardTitle } from './Card';
import { Skeleton } from '../skeleton/Skeleton';
import {
  SparkHoverTooltip,
  toneForValue,
  type SparkHoverPoint,
} from '../hover/SparkHoverTooltip';
import { useCursorTooltip } from '../../hooks/useCursorTooltip';

interface Props {
  total: number;
  delta: number;
  rangeLabel: string;
  spark: SparkPoint[];
  className?: string;
  loading?: boolean;
}

const fmtTime = (ts: number) => {
  const d = new Date(ts);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
};

export function TotalAlertsCard({ total, delta, rangeLabel, spark, className, loading }: Props) {
  const up = delta >= 0;
  const cardRef = useRef<HTMLDivElement>(null);
  const tipRef = useRef<HTMLDivElement>(null);
  const [point, setPoint] = useState<SparkHoverPoint | null>(null);

  useCursorTooltip(cardRef, tipRef, point !== null);

  const chartData: AreaPoint[] = spark.map(p => ({
    value: p.value,
    ts: p.ts,
    timeLabel: fmtTime(p.ts),
  }));

  if (loading) {
    return (
      <Card className={`card--total ${className ?? ''}`}>
        <CardTitle hint={rangeLabel}>Total Alerts</CardTitle>
        <div style={{ padding: 16, flex: 1, display: 'flex', flexDirection: 'column', gap: 12 }}>
          <Skeleton width="40%" height={48} />
          <Skeleton width="60%" height={16} />
        </div>
        <div style={{ height: 64, padding: '0 4px', display: 'flex', alignItems: 'flex-end' }}>
          <Skeleton width="100%" height="100%" borderRadius={0} />
        </div>
      </Card>
    );
  }

  return (
    <Card className={`card--total ${className ?? ''}`} ref={cardRef}>
      <CardTitle hint={rangeLabel}>Total Alerts</CardTitle>

      <div className="total-body">
        <div className="total-meta">
          <div className="total-value">{total}</div>
          <div className={`total-delta total-delta--${up ? 'up' : 'down'}`}>
            <ArrowUpIcon
              size={11}
              style={{ transform: up ? 'none' : 'rotate(180deg)' }}
            />
            <span>
              {up ? '+' : ''}{delta} vs previous {rangeLabel.toLowerCase()}
            </span>
          </div>
        </div>
      </div>

      <div className="spark-host">
        <AreaChart
          data={chartData}
          height={64}
          padding={{ top: 6, right: 4, bottom: 0, left: 4 }}
          lineColor="#FF7A6F"
          lineWidth={1.75}
          fillFrom="rgba(255, 122, 111, 0.40)"
          fillTo="rgba(255, 77, 79, 0.04)"
          showCrosshair={false}
          showHoverDot={false}
          interactionRef={cardRef}
          onHoverChange={(idx: number | null, p: AreaPoint | null) => {
            if (idx === null || !p) {
              setPoint(null);
              return;
            }
            setPoint({
              value: p.value,
              timeLabel: p.timeLabel ?? '',
              tone: toneForValue(p.value),
            });
          }}
        />
      </div>

      <SparkHoverTooltip ref={tipRef} point={point} />
    </Card>
  );
}
