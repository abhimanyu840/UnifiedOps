import type { TrendPoint } from '../../types';
import { UPlotLineChart } from '../charts/UPlotLineChart';
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
      <div className="trend-host">
        <UPlotLineChart
          data={data.map(d => ({ value: d.value, label: d.label, ts: d.ts }))}
          height={130}
          lineColor="#FF7A6F"
          fillFrom="rgba(255, 122, 111, 0.4)"
          fillTo="rgba(255, 77, 79, 0.05)"
          yLabel="alerts"
        />
      </div>
    </Card>
  );
}
