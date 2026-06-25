import type { HealthVendor, VendorFilter } from '../../types';
import type { ListenerRow } from '../../hooks/useListenerHealth';
import {
  BrocadeBadge,
  DellBadge,
  HitachiBadge,
  NetAppBadge,
  TotalBadge,
} from '../icons/VendorIcons';
import { Skeleton } from '../skeleton/Skeleton';

interface Props {
  vendors: HealthVendor[];
  selected: VendorFilter;
  onSelect: (next: VendorFilter) => void;
  downListenersByVendor?: Record<string, ListenerRow[]>;
  infraDownListenersByVendor?: Record<string, ListenerRow[]>;
  loading?: boolean;
}

export function SystemHealthOverview({
  vendors,
  selected,
  onSelect,
  downListenersByVendor,
  infraDownListenersByVendor,
  loading,
}: Props) {
  if (loading) {
    return (
      <section className="health-section">
        <div className="health-grid">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="health-card">
              <div className="health-card__icon">
                <Skeleton width={40} height={40} borderRadius="50%" />
              </div>
              <div className="health-card__body" style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 8 }}>
                <Skeleton width="60%" height={20} />
                <Skeleton width="40%" height={32} />
                <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
                  <Skeleton width="30%" height={16} />
                  <Skeleton width="30%" height={16} />
                </div>
              </div>
            </div>
          ))}
        </div>
      </section>
    );
  }

  return (
    <section className="health-section">
      <div className="health-grid">
        {vendors.map(v => (
          <VendorCard
            key={v.key}
            v={v}
            active={selected === v.key}
            downListeners={downListenersByVendor?.[v.key] ?? []}
            infraDownListeners={infraDownListenersByVendor?.[v.key] ?? []}
            onClick={() => onSelect(v.key)}
          />
        ))}
      </div>
    </section>
  );
}

interface VendorCardProps {
  v: HealthVendor;
  active: boolean;
  downListeners: ListenerRow[];
  infraDownListeners: ListenerRow[];
  onClick: () => void;
}

interface ListenerWarningBlockProps {
  downSites:      string[];
  infraDownSites: string[];
}

function ListenerWarningBlock({ downSites, infraDownSites }: ListenerWarningBlockProps) {
  if (downSites.length === 0 && infraDownSites.length === 0) return null;
  return (
    <div className="health-card__listener-warning">
      {infraDownSites.map(site => (
        <span
          key={`infra-${site}`}
          className="health-card__listener-warning-line health-card__listener-warning-line--infra"
        >
          <span className="health-card__listener-warning-icon" aria-hidden="true">⚠</span>
          {site} heartbeat store unreachable
        </span>
      ))}
      {downSites.map(site => (
        <span
          key={`down-${site}`}
          className="health-card__listener-warning-line"
        >
          <span className="health-card__listener-warning-icon" aria-hidden="true">⚠</span>
          {site} listener not running
        </span>
      ))}
    </div>
  );
}

function VendorCard({
  v, active, downListeners, infraDownListeners, onClick,
}: VendorCardProps) {
  const offline = !v.dataActive;
  const hasDownListener  = downListeners.length > 0;
  const hasInfraDown     = infraDownListeners.length > 0;
  const cls = [
    'health-card',
    `health-card--${v.icon}`,
    active           ? 'health-card--active'   : '',
    offline          ? 'health-card--offline'  : '',
    hasDownListener  ? 'health-card--listener-down' : '',
    hasInfraDown     ? 'health-card--infra-down'    : '',
  ].filter(Boolean).join(' ');

  // Dedupe by site so the "BCP / CDVL / SIFY listener not running" lines
  // don't list one entry per vendor-on-that-site (e.g. when Total Systems
  // collects across all vendors).
  const downSites      = Array.from(new Set(downListeners.map(d => d.site)));
  const infraDownSites = Array.from(new Set(infraDownListeners.map(d => d.site)));

  return (
    <button
      type="button"
      className={cls}
      onClick={onClick}
      aria-pressed={active}
      title={offline ? `${v.name}: no data — listener not configured` : `Show ${v.name}`}
    >
      <div className="health-card__icon">
        <VendorIcon kind={v.icon} />
      </div>
      <div className="health-card__body">
        <div className="health-card__name">{v.name}</div>

        {offline ? (
          <>
            <div className="health-card__count health-card__count--offline">—</div>
            <div className="health-card__breakdown">
              <span className="health-pill health-pill--offline">
                <span className="health-pill__dot" />
                No data feed
              </span>
            </div>
            <ListenerWarningBlock
              downSites={downSites}
              infraDownSites={infraDownSites}
            />
          </>
        ) : (
          <>
            <div className="health-card__count">{v.count}</div>
            <div className="health-card__breakdown">
              {hasInfraDown && !hasDownListener && (
                <span className="health-pill health-pill--unknown">
                  <span className="health-pill__dot" />
                  Status unknown
                </span>
              )}
              <span className="health-pill health-pill--critical">
                <span className="health-pill__dot" />
                {v.critical} Critical
              </span>
              <span className="health-pill health-pill--warning">
                <span className="health-pill__dot" />
                {v.warning} Warning
              </span>
            </div>
            <ListenerWarningBlock
              downSites={downSites}
              infraDownSites={infraDownSites}
            />
          </>
        )}
      </div>
    </button>
  );
}

function VendorIcon({ kind }: { kind: HealthVendor['icon'] }) {
  switch (kind) {
    case 'hitachi': return <HitachiBadge size={40} />;
    case 'netapp':  return <NetAppBadge  size={40} />;
    case 'dell':    return <DellBadge    size={40} />;
    case 'brocade': return <BrocadeBadge size={40} />;
    case 'total':   return <TotalBadge   size={40} />;
  }
}
