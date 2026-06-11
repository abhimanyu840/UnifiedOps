import type { TrendPoint } from '../../types';
import { AreaChart } from '../charts/AreaChart';
import { Card, CardTitle } from './Card';

interface Props {
  data: TrendPoint[];
  rangeLabel: string;
  className?: string;
}

export function AlertTrendCard({ data, rangeLabel, className }: Props) {
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
              borderRadius: '8px',
              padding: '10px 12px',
              boxShadow: '0 8px 30px rgba(0,0,0,0.4)',
              backdropFilter: 'blur(8px)',
              minWidth: '110px',
              display: 'flex',
              flexDirection: 'column',
              gap: '6px'
            }}>
              <span style={{ fontSize: '10px', color: '#94a3b8', fontWeight: 700, letterSpacing: '0.05em', textTransform: 'uppercase' }}>
                {p.timeLabel}
              </span>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#fafafa', fontWeight: 700, fontSize: '13px' }}>
                <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', background: '#FF7A6F', boxShadow: '0 0 8px rgba(255, 122, 111, 0.8)' }} />
                {p.value} alerts
              </div>
            </div>
          )}
        />
      </div>
    </Card>
  );
}
