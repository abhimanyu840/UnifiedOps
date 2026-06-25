import type { TrendPoint } from '../../types';
import { AreaChart } from '../charts/AreaChart';
import { Card, CardTitle } from './Card';
import { Skeleton } from '../skeleton/Skeleton';

interface Props {
  data: TrendPoint[];
  rangeLabel: string;
  className?: string;
  loading?: boolean;
}

export function AlertTrendCard({ data, rangeLabel, className, loading }: Props) {
  if (loading) {
    return (
      <Card className={`card--trend ${className ?? ''}`}>
        <CardTitle hint={rangeLabel}>Alert Trend</CardTitle>
        <div style={{ padding: 16, height: 130 }}>
          <Skeleton width="100%" height="100%" />
        </div>
      </Card>
    );
  }

  return (
    <Card className={`card--trend ${className ?? ''}`}>
      <CardTitle hint={rangeLabel}>Alert Trend</CardTitle>
      <div className="trend-host" style={{ paddingBottom: '16px' }}>
        <AreaChart
          data={data.map(d => ({ value: d.value, timeLabel: d.label, ts: d.ts }))}
          height={130}
          lineColor="#FF7A6F"
          fillFrom="rgba(255, 122, 111, 0.4)"
          fillTo="rgba(255, 77, 79, 0.05)"
          showXAxis={true}
          showYAxis={true}
          yTickCount={4}
          xTickCount={6}
          smooth={true}
          renderTooltip={(p) => (
            <div style={{
              background: 'rgba(30, 35, 43, 0.8)',
              border: '1px solid #2d333b',
              borderRadius: '6px',
              padding: '6px 8px',
              boxShadow: '0 4px 20px rgba(0,0,0,0.4)',
              backdropFilter: 'blur(8px)',
              display: 'flex',
              flexDirection: 'column',
              gap: '4px'
            }}>
              <span style={{ fontSize: '9px', color: '#94a3b8', fontWeight: 600, letterSpacing: '0.04em', textTransform: 'uppercase' }}>
                {p.timeLabel}
              </span>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px', color: '#fafafa', fontWeight: 700, fontSize: '12px' }}>
                <span style={{ display: 'inline-block', width: '6px', height: '6px', borderRadius: '50%', background: '#FF7A6F', boxShadow: '0 0 6px rgba(255, 122, 111, 0.8)' }} />
                {p.value} alerts
              </div>
            </div>
          )}
        />
      </div>
    </Card>
  );
}
