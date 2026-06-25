import { useState } from 'react';
import type { SystemStatus } from '../../types';

export interface ServiceStatusRow {
  /** Short label rendered as the row title in the tooltip */
  label:  string;
  /** Optional one-line description appended below the label */
  detail?: string;
  /** Visual tag — drives the dot colour on each tooltip row */
  kind?:  'listener' | 'heartbeat' | 'alert-store';
  /** The state of this service to drive visual rendering */
  state: 'up' | 'down' | 'error';
}

interface Props {
  status: SystemStatus;
  /** List of all services to show in hover. Defaults to error/down services, but can include running ones. */
  services?: ServiceStatusRow[];
}

const LABELS: Record<SystemStatus, string> = {
  live: 'Live',
  fetching: 'Fetching',
  error: 'Error',
};

const KIND_COLOR: Record<NonNullable<ServiceStatusRow['kind']>, string> = {
  listener:    '#FBBF24',
  heartbeat:   '#F87171',
  'alert-store': '#F87171',
};

const STATE_COLOR: Record<ServiceStatusRow['state'], string> = {
  up: '#10B981',
  down: '#FBBF24',
  error: '#F87171',
};

const KIND_LABEL: Record<NonNullable<ServiceStatusRow['kind']>, string> = {
  listener:    'LISTENER',
  heartbeat:   'HEARTBEAT INFLUX',
  'alert-store': 'ALERT STORE',
};

export function LiveBadge({ status, services = [] }: Props) {
  const [open, setOpen] = useState(false);

  const downCount    = services.filter(s => s.state !== 'up').length;
  const hasDown      = downCount > 0;
  const hasServices  = services.length > 0;
  const renderStatus = hasDown && status === 'live' ? 'warn' : status;
  const renderLabel  = hasDown && status === 'live' ? 'Warn' : LABELS[status];

  return (
    <div
      className={`status-pill status-pill--${renderStatus} ${hasDown ? 'status-pill--has-down' : ''} ${hasServices ? 'status-pill--has-hover' : ''}`}
      role="status"
      onMouseEnter={() => hasServices && setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => hasServices && setOpen(true)}
      onBlur={() => setOpen(false)}
      tabIndex={hasServices ? 0 : -1}
      title={hasDown ? `${downCount} service${downCount === 1 ? '' : 's'} down — hover for details` : (hasServices ? 'Hover for active services' : undefined)}
    >
      <span className="status-pill__dot" />
      <span>{renderLabel}</span>
      {hasDown && (
        <span className="status-pill__badge" aria-label={`${downCount} down`}>
          {downCount}
        </span>
      )}

      {hasServices && open && (
        <div className="status-pill__tooltip" role="tooltip">
          <div className="status-pill__tooltip-head">
            {hasDown ? `${downCount} service${downCount === 1 ? '' : 's'} down` : 'All systems operational'}
          </div>
          <ul className="status-pill__tooltip-list">
            {services.sort((a, b) => a.state === 'up' ? 1 : -1).map((s, i) => {
              const color = STATE_COLOR[s.state];
              return (
                <li key={`${s.label}-${i}`} className="status-pill__tooltip-row">
                  <span className="status-pill__tooltip-dot" style={{ background: color }} />
                  <div className="status-pill__tooltip-text">
                    <div className="status-pill__tooltip-label">
                      <span className="status-pill__tooltip-kind" style={{ color: KIND_COLOR[s.kind ?? 'listener'] }}>
                        {KIND_LABEL[s.kind ?? 'listener']}
                      </span>
                      <span>{s.label}</span>
                    </div>
                    {s.detail && (
                      <div className="status-pill__tooltip-detail" style={s.state === 'up' ? { opacity: 0.6 } : {}}>
                        {s.detail}
                      </div>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}
