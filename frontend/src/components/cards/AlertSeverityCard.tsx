import type { SeverityRow } from '../../types';
import { ExternalIcon } from '../icons/Icons';
import { DonutChart } from '../charts/DonutChart';
import { Card, CardTitle } from './Card';
import { Skeleton } from '../skeleton/Skeleton';

interface Props {
  data: SeverityRow[];
  onView?: () => void;
  onSliceClick?: (row: SeverityRow) => void;
  className?: string;
  loading?: boolean;
}

export function AlertSeverityCard({ data, onView, onSliceClick, className, loading }: Props) {
  const total = data.reduce((acc, r) => acc + r.value, 0);
  const visible = data.filter(d => d.value > 0);

  if (loading) {
    return (
      <Card className={`card--severity ${className ?? ''}`}>
        <CardTitle>Alert Severity</CardTitle>
        <div className="severity-row" style={{ padding: 16, display: 'flex', gap: 24, alignItems: 'center' }}>
          <Skeleton width={170} height={170} borderRadius="50%" />
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 12 }}>
            <Skeleton width="100%" height={24} />
            <Skeleton width="80%" height={24} />
            <Skeleton width="90%" height={24} />
            <Skeleton width="70%" height={24} />
          </div>
        </div>
      </Card>
    );
  }

  return (
    <Card className={`card--severity ${className ?? ''}`}>
      <CardTitle
        action={
          <button type="button" className="card-title__action" onClick={onView}>
            click a slice to filter <ExternalIcon size={11} />
          </button>
        }
      >
        Alert Severity
      </CardTitle>

      <div className="severity-row">
        <div className="donut-host">
          <DonutChart
            data={visible.length ? visible : data}
            size={170}
            innerRadius={42}
            outerRadius={80}
            showPercentLabels
            onSliceClick={(s) =>
              onSliceClick?.(data.find(d => d.name === s.name) ?? data[0])
            }
          />
        </div>

        <div className="severity-legend">
          {data.map(row => {
            const pct = total > 0 ? (row.value / total) * 100 : 0;
            return (
              <div
                key={row.key}
                className="legend-row"
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
                  className="legend-row__swatch"
                  style={{ background: row.color, color: row.color }}
                />
                <span className="legend-row__label">{row.name}</span>
                <span className="legend-row__count tabular">{row.value}</span>
                <span className="legend-row__pct">{pct.toFixed(1)}%</span>
              </div>
            );
          })}
        </div>
      </div>
    </Card>
  );
}
