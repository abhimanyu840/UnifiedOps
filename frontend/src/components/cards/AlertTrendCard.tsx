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
            <div className="flex flex-col gap-1">
              <span className="text-[10px] text-slate-400">{p.timeLabel}</span>
              <div className="flex items-center gap-2 text-[#fafafa] font-medium text-xs">
                <span className="inline-block w-2 h-2 rounded-full" style={{ background: '#FF7A6F' }} />
                {p.value} alerts
              </div>
            </div>
          )}
        />
      </div>
    </Card>
  );
}
