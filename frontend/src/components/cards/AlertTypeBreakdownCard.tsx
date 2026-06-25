import type { AlertTypeRow } from '../../types';
import { ExternalIcon } from '../icons/Icons';
import { DonutChart } from '../charts/DonutChart';
import { Card, CardTitle } from './Card';
import { Skeleton } from '../skeleton/Skeleton';

interface Props {
  data: AlertTypeRow[];
  rangeLabel: string;
  onView?: () => void;
  onSliceClick?: (row: AlertTypeRow) => void;
  className?: string;
  loading?: boolean;
}

export function AlertTypeBreakdownCard({
  data,
  rangeLabel,
  onView,
  onSliceClick,
  className,
  loading,
}: Props) {
  const total = data.reduce((acc, r) => acc + r.value, 0);

  if (loading) {
    return (
      <Card className={`card--type ${className ?? ''}`}>
        <CardTitle hint={rangeLabel}>Alert Type Breakdown</CardTitle>
        <div className="type-row" style={{ padding: 16, display: 'flex', gap: 24, alignItems: 'center' }}>
          <Skeleton width={140} height={140} borderRadius="50%" />
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 12 }}>
            <Skeleton width="100%" height={20} />
            <Skeleton width="80%" height={20} />
            <Skeleton width="90%" height={20} />
            <Skeleton width="70%" height={20} />
          </div>
        </div>
      </Card>
    );
  }

  return (
    <Card className={`card--type ${className ?? ''}`}>
      <CardTitle
        hint={rangeLabel}
        action={
          <button type="button" className="card-title__action" onClick={onView}>
            View all <ExternalIcon size={11} />
          </button>
        }
      >
        Alert Type Breakdown
      </CardTitle>

      <div className="type-row">
        <div className="donut-host donut-host--md">
          <DonutChart
            data={data}
            size={140}
            innerRadius={36}
            outerRadius={64}
            showPercentLabels
            onSliceClick={(s) =>
              onSliceClick?.(data.find(d => d.name === s.name) ?? data[0])
            }
          />
        </div>
        <div className="type-legend">
          {data.map(row => {
            const pct = total > 0 ? (row.value / total) * 100 : 0;
            return (
              <div
                key={row.name}
                className="type-legend__item"
                role="button"
                tabIndex={0}
                onClick={() => onSliceClick?.(row)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    onSliceClick?.(row);
                  }
                }}
                title={`Show ${row.name} alerts`}
              >
                <span
                  className="type-legend__dot"
                  style={{ background: row.color }}
                />
                <span className="type-legend__label" title={row.name}>
                  {row.name}
                </span>
                <span className="type-legend__count tabular">{row.value}</span>
                <span className="type-legend__pct">{pct.toFixed(0)}%</span>
              </div>
            );
          })}
        </div>
      </div>
    </Card>
  );
}
