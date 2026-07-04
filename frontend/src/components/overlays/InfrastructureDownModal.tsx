import { useEffect } from 'react';
import { motion } from 'motion/react';
import type { InfraEvent } from '../../hooks/useListenerHealth';

interface Props {
  event:         InfraEvent;
  remaining:     number;
  onAcknowledge: () => void;
}

const fmtSince = (epoch: number): string => {
  const d = new Date(epoch * 1000);
  return d.toLocaleTimeString([], {
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
};

const fmtAge = (epoch: number): string => {
  const s = Math.max(0, (Date.now() / 1000) - epoch);
  if (s < 60)   return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
};

const VENDOR_LABEL: Record<string, string> = {
  hitachi: 'Hitachi',
  brocade: 'Brocade',
  netapp:  'NetApp',
  dell:    'Dell',
};

const titleCase = (s: string): string =>
  s ? s.charAt(0).toUpperCase() + s.slice(1) : s;

export function InfrastructureDownModal({ event, remaining, onAcknowledge }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' || e.key === 'Enter') onAcknowledge();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onAcknowledge]);

  const isAlertStore = event.component === 'alert';
  const vendorName   = event.vendor ? (VENDOR_LABEL[event.vendor] ?? titleCase(event.vendor)) : '';

  const heartbeatPort = event.site === 'CDVL' ? '8486' : event.site === 'BCP' ? '8487' : '8488';
  const containerName = isAlertStore
    ? `unifiedops-influx-${event.vendor}-${event.site.toLowerCase()}`
    : `unifiedops-influx-heartbeat-${event.site.toLowerCase()}`;

  return (
    <div className="modal listener-modal" role="dialog" aria-modal="true">
      <motion.div
        className="modal__card listener-modal__card infra-modal__card"
        initial={{ opacity: 0, y: 12, scale: 0.97 }}
        animate={{ opacity: 1, y: 0,  scale: 1    }}
        transition={{ duration: 0.18, ease: 'easeOut' }}
      >
        <div className="listener-modal__head infra-modal__head">
          <span className="listener-modal__dot infra-modal__dot" />
          <div>
            <div className="listener-modal__eyebrow infra-modal__eyebrow">
              {isAlertStore ? 'ALERT STORE UNREACHABLE' : 'HEARTBEAT INFRASTRUCTURE DOWN'}
            </div>
            <div className="listener-modal__title">
              {isAlertStore
                ? `${vendorName} ${event.site} Alert Store Unreachable`
                : `${event.site} Heartbeat Store Unreachable`}
            </div>
          </div>
        </div>

        <div className="listener-modal__body">
          {isAlertStore ? (
            <p className="listener-modal__lede">
              The <strong>{vendorName}</strong> alert InfluxDB at site{' '}
              <strong>{event.site}</strong> is not responding. New alerts from
              {' '}{vendorName} devices at this site <strong>cannot be queried
              or displayed</strong> until the container is restored. Listener
              heartbeats and other vendor buckets are unaffected.
            </p>
          ) : (
            <p className="listener-modal__lede">
              The heartbeat InfluxDB for site <strong>{event.site}</strong> is
              not responding. <strong>Listener status cannot be determined</strong>
              {' '}while the heartbeat store is down — the underlying syslog/SNMP
              listeners may still be running, but their heartbeats can&rsquo;t
              land. Alerts already in the alert buckets continue to flow.
            </p>
          )}

          <div className="listener-modal__rows">
            <div className="listener-modal__row">
              <span className="listener-modal__key">Site</span>
              <span className="listener-modal__val">{event.site}</span>
            </div>
            {isAlertStore && (
              <div className="listener-modal__row">
                <span className="listener-modal__key">Vendor</span>
                <span className="listener-modal__val">{vendorName}</span>
              </div>
            )}
            <div className="listener-modal__row">
              <span className="listener-modal__key">Component</span>
              <span className="listener-modal__val">
                {isAlertStore ? `${vendorName} Alert InfluxDB` : 'Heartbeat InfluxDB'}
              </span>
            </div>
            <div className="listener-modal__row">
              <span className="listener-modal__key">Down since</span>
              <span className="listener-modal__val cell-mono">{fmtSince(event.since)}</span>
            </div>
            <div className="listener-modal__row">
              <span className="listener-modal__key">Duration</span>
              <span className="listener-modal__val cell-mono">{fmtAge(event.since)}</span>
            </div>
            <div className="listener-modal__row">
              <span className="listener-modal__key">Reason</span>
              <span className="listener-modal__val cell-mono">{event.error || 'unreachable'}</span>
            </div>
          </div>

          <div className="listener-modal__hint infra-modal__hint">
            {isAlertStore ? (
              <>
                <strong>What to do:</strong> The {vendorName} alert InfluxDB
                container for site <strong>{event.site}</strong> is unreachable.
                Verify the container is running (<code>podman ps</code>) and
                restart with{' '}
                <code>podman start {containerName}</code>.
                New alerts will resume flowing into the dashboard automatically
                once the bucket is back online.
              </>
            ) : (
              <>
                <strong>What to do:</strong> The heartbeat InfluxDB instance for
                {' '}<strong>{event.site}</strong> is unreachable. Verify the
                container is running (<code>podman ps</code>) and the host on
                port {heartbeatPort} is reachable, then restart with{' '}
                <code>podman start {containerName}</code>.
                Listener heartbeats will resume on their own once the store is back.
              </>
            )}
          </div>
        </div>

        <div className="listener-modal__foot">
          {remaining > 0 && (
            <span className="listener-modal__queue">
              {remaining} more {remaining === 1 ? 'outage' : 'outages'} queued
            </span>
          )}
          <button
            type="button"
            className="btn-primary listener-modal__btn"
            onClick={onAcknowledge}
            autoFocus
          >
            Acknowledge
          </button>
        </div>
      </motion.div>
    </div>
  );
}
