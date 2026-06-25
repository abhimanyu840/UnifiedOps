import type { Location, SystemRow } from '../../types';
import { ExternalIcon } from '../icons/Icons';
import { Card, CardTitle } from './Card';
import { Skeleton } from '../skeleton/Skeleton';

interface Props {
  systems: SystemRow[];
  /** Max rows to display for the currently selected tab. Default 9. */
  topN?: number;
  /** Globally selected locations (drives both the topbar dropdown
   *  and the tab highlight on this card). */
  selectedLocations: Location[];
  /** Universe of locations the user can choose between. */
  allLocations: Location[];
  /** Push the user's tab choice back up to the global location filter. */
  onLocationsChange: (next: Location[]) => void;
  onView?: () => void;
  onSystemClick?: (row: SystemRow) => void;
  className?: string;
  loading?: boolean;
}

const LOC_COLOR: Record<Location | 'ALL', { from: string; to: string; dot: string }> = {
  ALL:  { from: '#475569', to: '#94a3b8', dot: '#94a3b8' },
  CDVL: { from: '#1E90FF', to: '#00C2FF', dot: '#1E90FF' },
  BCP:  { from: '#7C3AED', to: '#9B5CFF', dot: '#9B5CFF' },
  SIFY: { from: '#00B894', to: '#00D26A', dot: '#00D26A' },
};

export function TopSystemsCard({
  systems,
  topN = 100,
  selectedLocations,
  allLocations,
  onLocationsChange,
  onView,
  onSystemClick,
  className,
  loading,
}: Props) {
  // Active tab is derived from the global location filter:
  //   - all sites selected (or none)  -> "ALL"
  //   - exactly one site selected     -> that site
  //   - multi-select subset           -> "ALL" (no single tab matches)
  const isAll =
    selectedLocations.length === 0 ||
    selectedLocations.length === allLocations.length;
  const tab: Location | 'ALL' =
    isAll
      ? 'ALL'
      : selectedLocations.length === 1
        ? selectedLocations[0]
        : 'ALL';

  const tabs: Array<{ key: Location | 'ALL'; label: string }> = [
    { key: 'ALL', label: 'All' },
    ...allLocations.map(loc => ({ key: loc, label: loc })),
  ];

  const handleTab = (next: Location | 'ALL') => {
    onLocationsChange(next === 'ALL' ? [...allLocations] : [next]);
  };

  // Filter by tab FIRST, then take the top N for that tab. This way each
  // tab reveals the genuine leaderboard for its location instead of being
  // restricted to whichever rows made it into a single global top-N.
  const tabPool = tab === 'ALL'
    ? systems
    : systems.filter(s => s.location === tab);
  const filtered = tabPool.slice(0, topN);
  const tabTotal = tabPool.length;

  const max = Math.max(1, ...filtered.map(s => s.alerts));

  const useTwoCols = filtered.length > 5;
  const half = Math.ceil(filtered.length / 2);
  const col1 = useTwoCols ? filtered.slice(0, half) : filtered;
  const col2 = useTwoCols ? filtered.slice(half) : [];

  if (loading) {
    return (
      <Card className={`card--top-systems ${className ?? ''}`}>
        <CardTitle>Top Systems by Alerts</CardTitle>
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 16 }}>
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} width="100%" height={24} />
          ))}
        </div>
      </Card>
    );
  }

  return (
    <Card className={`card--top-systems ${className ?? ''}`}>
      <CardTitle
        hint={tabTotal > filtered.length
          ? `(top ${filtered.length} of ${tabTotal})`
          : `(${filtered.length} systems)`}
        action={
          <div className="row gap-2">
            <div className="top-tabs">
              {tabs.map(t => (
                <button
                  key={t.key}
                  type="button"
                  className={`top-tab ${tab === t.key ? 'top-tab--active' : ''}`}
                  onClick={() => handleTab(t.key)}
                  title={t.key === 'ALL'
                    ? 'Show every location'
                    : `Filter dashboard to ${t.key}`}
                >
                  <span
                    className="top-tab__dot"
                    style={{ background: LOC_COLOR[t.key].dot }}
                  />
                  {t.label}
                </button>
              ))}
            </div>
            <button type="button" className="card-title__action" onClick={onView}>
              View all systems <ExternalIcon size={11} />
            </button>
          </div>
        }
      >
        Top Systems by Alerts
      </CardTitle>

      <div className={`systems-grid ${useTwoCols ? '' : 'systems-grid--1col'}`}>
        <SystemColumn rows={col1} max={max} onClick={onSystemClick} />
        {useTwoCols && <SystemColumn rows={col2} max={max} onClick={onSystemClick} />}
      </div>
    </Card>
  );
}

interface ColProps {
  rows: SystemRow[];
  max: number;
  onClick?: (row: SystemRow) => void;
}

function SystemColumn({ rows, max, onClick }: ColProps) {
  return (
    <div className="systems-col">
      {rows.map(s => {
        const palette = LOC_COLOR[s.location as keyof typeof LOC_COLOR] || LOC_COLOR.ALL;
        const width = `${Math.max(6, (s.alerts / max) * 100)}%`;
        return (
          <div
            key={s.name}
            className="system-row"
            title={s.name}
            onClick={() => onClick?.(s)}
          >
            <span className="system-row__name">{s.name}</span>
            <div
              className="bar"
              style={{
                width,
                ['--bar-from' as string]: palette.from,
                ['--bar-to' as string]: palette.to,
              }}
            />
            <span className="system-row__value">{s.alerts}</span>
          </div>
        );
      })}
    </div>
  );
}
