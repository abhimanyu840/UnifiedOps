import type React from 'react';
import { Card, CardTitle } from './Card';
import { Skeleton } from '../skeleton/Skeleton';

interface Props {
  alertCount: number;
  rangeLabel: string;
  onView?: () => void;
  className?: string;
  loading?: boolean;
}

type Tone = 'ok' | 'warn' | 'crit';

const toneFor = (count: number): Tone => {
  if (count === 0) return 'ok';
  if (count >= 5) return 'crit';
  return 'warn';
};

export function NTPCard({ alertCount, rangeLabel, onView, className, loading }: Props) {
  const tone = toneFor(alertCount);

  if (loading) {
    return (
      <Card className={`card--ntp card--ntp--ok ${className ?? ''}`}>
        <CardTitle>NTP</CardTitle>
        <div style={{ padding: 16, flex: 1, display: 'flex', flexDirection: 'column', gap: 8 }}>
          <Skeleton width="30%" height={36} />
          <Skeleton width="70%" height={16} />
        </div>
      </Card>
    );
  }

  return (
    <Card
      className={`card--ntp card--ntp--${tone} card--clickable ${className ?? ''}`}
      role="button"
      tabIndex={0}
      onClick={onView}
      onKeyDown={(e: React.KeyboardEvent<HTMLDivElement>) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          onView?.();
        }
      }}
      title="Click to view NTP / time-sync alerts"
    >
      <CardTitle>NTP</CardTitle>

      <div className="ntp-body">
        <div className="ntp-count">
          <span className="ntp-count__value">{alertCount}</span>
          <span className="ntp-count__sub">
            {alertCount === 1 ? 'alert' : 'alerts'} in {rangeLabel.toLowerCase()}
          </span>
        </div>
      </div>

      <div className="ntp-cta-row">
        <span className="ntp-cta-text">CLICK TO VIEW DETAILS</span>
      </div>
    </Card>
  );
}
