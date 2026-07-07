import type React from 'react';
import type { RecentAlert } from '../../types';
import { ExternalIcon } from '../icons/Icons';
import { Card, CardTitle } from './Card';
import { AlertsDataTable } from '../tables/AlertsDataTable';

interface Props {
  alerts: RecentAlert[];
  rangeLabel: string;
  onView?: () => void;
  className?: string;
  loading?: boolean;
}

export function RecentAlertsCard({
  alerts,
  rangeLabel,
  onView,
  className,
  loading,
}: Props) {
  return (
    <Card
      className={`card--recent card--clickable ${className ?? ''}`}
      role="button"
      tabIndex={0}
      onClick={onView}
      onKeyDown={(e: React.KeyboardEvent<HTMLDivElement>) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          onView?.();
        }
      }}
      title="Click to view all alerts"
    >
      <CardTitle
        hint={`(${alerts.length} rows)`}
        action={
          <button type="button" className="card-title__action">
            View all <ExternalIcon size={11} />
          </button>
        }
      >
        Recent Alerts
      </CardTitle>

      <AlertsDataTable
        alerts={alerts}
        variant="compact"
        loading={loading}
        emptyText={`No alerts in ${rangeLabel.toLowerCase()} (received ${alerts.length})`}
      />
    </Card>
  );
}
