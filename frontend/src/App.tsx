import { useMemo, useEffect, useState, useRef } from 'react';
// Microservice imports — go through each service's `index.ts` barrel,
// never reach into a service's internals from another service.
import { Header, NTPCard }                from './services/dashboard';
import { SystemHealthOverview }           from './services/system-health';
import {
  AlertDetailsModal,
  AlertSeverityCard,
  AlertTrendCard,
  AlertTypeBreakdownCard,
  RecentCriticalAlertsCard,
  TopSystemsCard,
  TotalAlertsCard,
  useAlertsStore,
  useDashboardStore,
  useFilterStore,
  useModalStore,
  useUiStore,
} from './services/alerts';
import {
  ConnectionLostModal,
  InfrastructureDownModal,
  ListenerDownModal,
  useListenerHealthStore,
  useWsConnector,
  type ListenerRow,
} from './services/listener-health';
import {
  ScreenBlink,
  ToastStack,
} from './services/ui-kit';
import {
  ALERT_TYPE_PALETTE,
  HEALTH_VENDORS,
  RANGE_OPTIONS,
  SEVERITY_PALETTE,
} from './data/config';
import { bestCategory, normalizeCategory } from './utils/category';
import { ReportPanel } from './components/ReportPanel';
import type {
  Location,
  RecentAlert,
  Severity,
  TimeRange,
} from './types';

const ALL_LOCATIONS: Location[] = ['CDVL', 'BCP', 'SIFY'];

const SEVERITY_COLOR: Record<Severity, string> = {
  critical:      '#ef4444',
  error:         '#f97066',
  warning:       '#f97316',
  notice:        '#eab308',
  informational: '#3b82f6',
};

const rangeLabel = (r: TimeRange): string => {
  if (r.kind === 'relative') {
    return RANGE_OPTIONS.find(o => o.key === r.key)?.label ?? r.key;
  }
  const s = new Date(r.start);
  const e = new Date(r.stop);
  const fmt = (d: Date) =>
    `${d.toLocaleDateString([], { month: 'short', day: 'numeric' })} ${d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
  return `${fmt(s)} → ${fmt(e)}`;
};

// Rolling buffer cap lives in the zustand alerts store now (MAX_BUFFER).

const MS_PER_MIN  = 60 * 1000;
const MS_PER_HOUR = 60 * MS_PER_MIN;
const MS_PER_DAY  = 24 * MS_PER_HOUR;

// Duration in ms for every range option from the header dropdown.
// Single source of truth — `rangeWindowMs` reads from here.
const RANGE_DURATIONS_MS: Record<string, number> = {
  '5m':   5  * MS_PER_MIN,
  '15m':  15 * MS_PER_MIN,
  '30m':  30 * MS_PER_MIN,
  '1h':   1  * MS_PER_HOUR,
  '3h':   3  * MS_PER_HOUR,
  '6h':   6  * MS_PER_HOUR,
  '12h':  12 * MS_PER_HOUR,
  '24h':  24 * MS_PER_HOUR,
  '1d':   1  * MS_PER_DAY,
  '2d':   2  * MS_PER_DAY,
  '3d':   3  * MS_PER_DAY,
  '7d':   7  * MS_PER_DAY,
  '15d':  15 * MS_PER_DAY,
  '30d':  30 * MS_PER_DAY,
};

// Number of buckets used to render the Total-Alerts spark + the Alert-Trend
// curve. Tuned per range so a 5-minute window doesn't get cut into 24
// 12-second slices (where rounding pushes every alert to the last bucket)
// while a 30-day window still has enough resolution.
const BUCKETS_PER_RANGE: Record<string, number> = {
  '5m':   10,
  '15m':  15,
  '30m':  15,
  '1h':   12,
  '3h':   18,
  '6h':   24,
  '12h':  24,
  '24h':  24,
  '1d':   24,
  '2d':   24,
  '3d':   24,
  '7d':   28,
  '15d':  30,
  '30d':  30,
};

const formatBucketLabel = (d: Date, durationMs: number): string => {
  if (durationMs > 36 * MS_PER_HOUR) {
    return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
  }
  const hh   = d.getHours();
  const mm   = d.getMinutes();
  const h12  = ((hh + 11) % 12) + 1;
  const ampm = hh < 12 ? 'am' : 'pm';
  return `${String(h12).padStart(2, '0')}:${String(mm).padStart(2, '0')} ${ampm}`;
};

const rangeWindowMs = (
  range: TimeRange,
): { endMs: number; durationMs: number; bucketCount: number } => {
  const now = Date.now();
  if (range.kind === 'relative') {
    const dur = RANGE_DURATIONS_MS[range.key] ?? 6 * MS_PER_HOUR;
    return {
      endMs:       now,
      durationMs:  dur,
      bucketCount: BUCKETS_PER_RANGE[range.key] ?? 24,
    };
  }
  const startMs = new Date(range.start).getTime();
  const endMs   = new Date(range.stop).getTime();
  const dur     = Math.max(MS_PER_MIN, endMs - startMs);
  // Pick a bucket count proportional to duration so very short custom
  // windows don't get over-resolved and 30-day windows still have detail.
  let bucketCount = 24;
  if      (dur <=  10 * MS_PER_MIN)  bucketCount = 10;
  else if (dur <=  60 * MS_PER_MIN)  bucketCount = 12;
  else if (dur <=  6  * MS_PER_HOUR) bucketCount = 24;
  else if (dur <=  3  * MS_PER_DAY)  bucketCount = 24;
  else if (dur <=  14 * MS_PER_DAY)  bucketCount = 28;
  else                               bucketCount = 30;
  return { endMs, durationMs: dur, bucketCount };
};

/**
 * Bucket the supplied alerts into N equal time slices across the active
 * range. Counts are derived strictly from `alert.ts` (epoch ms), so the
 * area-under-curve always equals the count of alerts inside the range —
 * Total Alerts, Spark, and Trend reconcile to the same underlying data.
 */
const bucketAlerts = (
  alerts: RecentAlert[],
  endMs: number,
  durationMs: number,
  bucketCount: number,
): Array<{ ts: number; value: number; label: string }> => {
  const startMs     = endMs - durationMs;
  const bucketWidth = durationMs / bucketCount;
  const counts      = new Array<number>(bucketCount).fill(0);
  for (const a of alerts) {
    if (a.ts < startMs || a.ts > endMs) continue;
    const idx = Math.min(
      bucketCount - 1,
      Math.max(0, Math.floor((a.ts - startMs) / bucketWidth)),
    );
    counts[idx]++;
  }
  return counts.map((value, i) => {
    const ts = startMs + (i + 0.5) * bucketWidth;
    return {
      ts,
      value,
      label: formatBucketLabel(new Date(ts), durationMs),
    };
  });
};

export default function App() {
  // -------- zustand-backed filter / chrome / modal state -------------
  const range            = useFilterStore(s => s.range);
  const locations        = useFilterStore(s => s.locations);
  const selectedVendor   = useFilterStore(s => s.selectedVendor);
  const setRange         = useFilterStore(s => s.setRange);
  const setLocations     = useFilterStore(s => s.setLocations);
  const setSelectedVendor= useFilterStore(s => s.setSelectedVendor);

  const toasts           = useUiStore(s => s.toasts);
  const pushToast        = useUiStore(s => s.pushToast);
  const dismissToast     = useUiStore(s => s.dismissToast);
  const blinkColor       = useUiStore(s => s.blinkColor);
  const blinkKey         = useUiStore(s => s.blinkKey);
  const triggerBlink     = useUiStore(s => s.triggerBlink);
  const status           = useUiStore(s => s.status);
  const setStatus        = useUiStore(s => s.setStatus);
  const refreshing       = useUiStore(s => s.refreshing);
  const setRefreshing    = useUiStore(s => s.setRefreshing);

  const modalOpen        = useModalStore(s => s.modalOpen);
  const modalFilters     = useModalStore(s => s.modalFilters);
  const openModal        = useModalStore(s => s.openModal);
  const closeModal       = useModalStore(s => s.closeModal);
  const ackedKeys        = useModalStore(s => s.ackedKeys);
  const ackedInfraKeys   = useModalStore(s => s.ackedInfraKeys);
  const acknowledgeKey   = useModalStore(s => s.acknowledgeKey);
  const acknowledgeInfra = useModalStore(s => s.acknowledgeInfraKey);

  // --- listener health (heartbeat-driven, WS-push via zustand) ---
  // Per-field selectors so each one returns a stable reference from
  // the store. Returning a fresh object literal from a single selector
  // breaks React 18's getSnapshot caching contract and blanks the tree.
  const lhListeners    = useListenerHealthStore(s => s.listeners);
  const lhSites        = useListenerHealthStore(s => s.sites);
  const lhDownEvents   = useListenerHealthStore(s => s.downEvents);
  const lhInfraEvents  = useListenerHealthStore(s => s.infraEvents);
  const lhStatus       = useListenerHealthStore(s => s.wsStatus);
  const lhStatusSince  = useListenerHealthStore(s => s.statusSince);

  // Build the legacy `listenerHealth` shape from individual selectors
  // so existing code below (which still destructures it) keeps working.
  const listenerHealth = useMemo(() => ({
    listeners:    lhListeners,
    sites:        lhSites,
    down_events:  lhDownEvents,
    infra_events: lhInfraEvents,
  }), [lhListeners, lhSites, lhDownEvents, lhInfraEvents]);

  // Ack key sync is handled INSIDE the listener-health store's `ingest`
  // (it calls `useModalStore.syncAcks` after every frame), so App.tsx
  // doesn't need an effect for it any more.
  const queuedDownEvents = useMemo(
    () => listenerHealth.down_events.filter(e => !ackedKeys.has(e.key)),
    [listenerHealth.down_events, ackedKeys],
  );
  const queuedInfraEvents = useMemo(
    () => listenerHealth.infra_events.filter(e => !ackedInfraKeys.has(e.key)),
    [listenerHealth.infra_events, ackedInfraKeys],
  );

  // Combined "what's down right now" feed for the LIVE pill in the header.
  // Surfaces every listener that has missed its 3-beat threshold, every
  // heartbeat InfluxDB that is unreachable, and every alert-store
  // container that is unreachable — each becomes one row in the hover
  // tooltip so the operator sees the full impact at a glance.
  const servicesForPill = useMemo(() => {
    const out: { label: string; detail?: string; kind: 'listener' | 'heartbeat' | 'alert-store'; state: 'up' | 'down' | 'error' }[] = [];
    for (const ev of listenerHealth.infra_events) {
      if (ev.component === 'alert') {
        const v = String(ev.vendor ?? '').trim();
        const vendorPretty = v ? v.charAt(0).toUpperCase() + v.slice(1) : 'Vendor';
        out.push({
          kind: 'alert-store',
          state: 'error',
          label: `${vendorPretty} ${ev.site}`,
          detail: ev.error || 'alert store unreachable',
        });
      } else {
        out.push({
          kind: 'heartbeat',
          state: 'error',
          label: `${ev.site} heartbeat InfluxDB`,
          detail: ev.error || 'unreachable',
        });
      }
    }
    for (const l of listenerHealth.listeners) {
      if (l.state === 'unknown' || l.state === 'infra_down') continue;
      if (l.state === 'down') {
        out.push({
          kind: 'listener',
          state: 'down',
          label: `${l.oem} ${l.site}`,
          detail: `${l.listener} — no heartbeat for ${l.age_s ? Math.round(l.age_s) + 's' : '>30s'}`,
        });
      } else if (l.state === 'up') {
        out.push({
          kind: 'listener',
          state: 'up',
          label: `${l.oem} ${l.site}`,
          detail: `${l.listener} — receiving telemetry`,
        });
      }
    }
    return out;
  }, [listenerHealth.infra_events, listenerHealth.listeners]);

  // Infrastructure outages take priority over listener-down events: when the
  // heartbeat store is unreachable, listener state is fundamentally unknown,
  // so it would be confusing to show a "listener not running" modal on top
  // of an "InfluxDB unreachable" condition.
  const activeInfraModal      = queuedInfraEvents[0] ?? null;
  const remainingInInfraQueue = Math.max(0, queuedInfraEvents.length - 1);

  const activeListenerModal   = activeInfraModal ? null : (queuedDownEvents[0] ?? null);
  const remainingInModalQueue = Math.max(0, queuedDownEvents.length - 1);

  const acknowledgeActiveModal = () => {
    if (activeListenerModal) acknowledgeKey(activeListenerModal.key);
  };
  const acknowledgeActiveInfra = () => {
    if (activeInfraModal) acknowledgeInfra(activeInfraModal.key);
  };

  // Listeners that are positively DOWN (heartbeat store reachable but no
  // recent heartbeat) — these get the amber "listener not running" warning.
  const downListenersByVendor = useMemo(() => {
    const map: Record<string, ListenerRow[]> = {
      hitachi: [], brocade: [], netapp: [], dell: [], total: [],
    };
    for (const l of listenerHealth.listeners) {
      if (l.state !== 'down') continue;
      const oemKey = l.oem.toLowerCase();
      if (map[oemKey]) map[oemKey].push(l);
      map.total.push(l);
    }
    return map;
  }, [listenerHealth.listeners]);

  // Listeners whose site heartbeat store is unreachable — these get the red
  // "Heartbeat store unreachable" warning. Status is genuinely UNKNOWN, not
  // DOWN, because we can't currently observe them.
  const infraDownListenersByVendor = useMemo(() => {
    const map: Record<string, ListenerRow[]> = {
      hitachi: [], brocade: [], netapp: [], dell: [], total: [],
    };
    for (const l of listenerHealth.listeners) {
      if (l.state !== 'infra_down') continue;
      const oemKey = l.oem.toLowerCase();
      if (map[oemKey]) map[oemKey].push(l);
      map.total.push(l);
    }
    return map;
  }, [listenerHealth.listeners]);

  const vendorActive = useMemo(() => {
    if (selectedVendor === 'total') return true;
    return HEALTH_VENDORS.find(v => v.key === selectedVendor)?.dataActive ?? false;
  }, [selectedVendor]);

  const label = rangeLabel(range);

  const vendorName = useMemo(() => {
    if (selectedVendor === 'total') return null;
    return HEALTH_VENDORS.find(v => v.key === selectedVendor)?.name ?? selectedVendor;
  }, [selectedVendor]);

  // Alert WS drives toast + blink + the rolling buffer used for the
  // hover-card and the live "Recent Critical Alerts" table.
  const alertsStatus      = useAlertsStore(s => s.wsStatus);
  const alertsStatusSince = useAlertsStore(s => s.statusSince);

  // Dashboard cards read from THIS store. Filled by the /ws/dashboard
  // channel — backend's DashboardBroadcaster computes the snapshot on
  // every 5 s tick + immediately on (re)subscribe. No browser-side
  // Flux, no REST polling.
  const dashSnap     = useDashboardStore(s => s.snapshot);
  const dashHydrated = useDashboardStore(s => s.hydrated);

  // The subscription is just (range, sites). Memoised so the WS hook's
  // dep array only triggers a re-subscribe when something actually
  // changes.
  const subscription = useMemo(
    () => ({ range: range.kind === 'relative' ? range.key : '6h',
             sites:   [...locations],
             vendors: [] as string[] }),
    [range, locations],
  );

  // Show skeleton placeholders until the first dashboard frame lands,
  // or until the incoming snapshot's range and sites match our current subscription.
  const dashLoading = !dashHydrated || !dashSnap ||
                      dashSnap.range !== subscription.range ||
                      (dashSnap.sites || []).slice().sort().join(',') !== subscription.sites.slice().sort().join(',');

  useWsConnector({
    subscription,
    onNewAlerts: (fresh) => {
      const newest = fresh[fresh.length - 1];
      if (!newest) return;
      const tail = fresh.length > 1 ? ` (+${fresh.length - 1} more)` : '';
      pushToast({
        severity: newest.severity,
        title:    `${fresh.length} new alert${fresh.length === 1 ? '' : 's'}`,
        message:  `${newest.severity} · ${newest.storageName} (${newest.ip})${tail}`,
      });
      triggerBlink(SEVERITY_COLOR[newest.severity]);
      // No manual refresh needed — the backend broadcaster will push a
      // fresh dashboard snapshot on its next tick (≤ 5 s); the toast +
      // blink already gave the operator the immediate confirmation.
    },
  });

  // Authoritative snapshot from the InfluxDB queries store. `rangeAlerts`
  // is what every card consumes; it already honours range + locations
  // because the query itself was scoped by them. We then filter
  // client-side by the active vendor pill so toggling vendors doesn't
  // re-query.
  const rangeAlerts = useMemo<RecentAlert[]>(
    () => dashSnap?.recent ?? [],
    [dashSnap],
  );

  const filteredAlerts = useMemo<RecentAlert[]>(() => {
    if (!vendorActive) return [];
    return selectedVendor === 'total'
      ? rangeAlerts
      : rangeAlerts.filter(a => a.vendor === selectedVendor);
  }, [rangeAlerts, selectedVendor, vendorActive]);

  // Severity counts: use the authoritative breakdown when the active
  // pill is "total"; otherwise count from `filteredAlerts` (vendor-only).
  const severityData = useMemo(() => {
    const counts: Record<Severity, number> = {
      critical: 0, error: 0, warning: 0, notice: 0, informational: 0,
    };
    if (selectedVendor === 'total' && dashSnap) {
      counts.critical      = dashSnap.severity.critical;
      counts.error         = dashSnap.severity.error;
      counts.warning       = dashSnap.severity.warning;
      counts.notice        = dashSnap.severity.notice;
      counts.informational = dashSnap.severity.informational;
    } else {
      for (const a of filteredAlerts) counts[a.severity]++;
    }
    return SEVERITY_PALETTE.map(row => ({ ...row, value: counts[row.key] }));
  }, [dashSnap, filteredAlerts, selectedVendor]);

  const alertTypeData = useMemo(() => {
    // Bucket every alert into one of the 12 display categories.
    //
    // Strategy:
    //   1. If the backend's group-by-tag aggregation has at least one
    //      categorised row, use it as the authoritative count for the
    //      total view (no field duplicates, full range coverage).
    //   2. ELSE derive per-row from `filteredAlerts` using bestCategory
    //      (raw tag first, body-text inference as fallback). This is
    //      what fires when a listener doesn't set trap_category at all
    //      (NetApp / Dell today) or when the user has filtered to a
    //      specific vendor.
    //
    // Either way, every "other"/unknown row gets a best-effort body
    // categorisation so the chart never goes blank.
    const counts = new Map<string, number>();
    const useAggregated =
      selectedVendor === 'total'
      && dashSnap
      && Object.keys(dashSnap.categories).length > 0;

    if (useAggregated) {
      for (const [raw, n] of Object.entries(dashSnap!.categories)) {
        const bucket = normalizeCategory(raw);
        counts.set(bucket, (counts.get(bucket) ?? 0) + n);
      }
    } else {
      for (const a of filteredAlerts) {
        const bucket = bestCategory(a.category, a.event);
        counts.set(bucket, (counts.get(bucket) ?? 0) + 1);
      }
    }
    return ALERT_TYPE_PALETTE.map(row => ({ ...row, value: counts.get(row.name) ?? 0 }));
  }, [dashSnap, filteredAlerts, selectedVendor]);

  // Top systems: use the authoritative Flux topN when the active pill
  // is "total"; otherwise derive from the vendor-filtered alert stream
  // so switching vendors surfaces only that vendor's systems.
  const filteredSystems = useMemo(() => {
    if (selectedVendor === 'total' && dashSnap) {
      return dashSnap.topSystems.map(s => ({
        name:     s.name,
        alerts:   s.alerts,
        location: s.location,
        vendor:   s.vendor,
      }));
    }
    const byStorage = new Map<string, {
      name:     string;
      alerts:   number;
      location: typeof filteredAlerts[number]['location'];
      vendor:   typeof filteredAlerts[number]['vendor'];
    }>();
    for (const a of filteredAlerts) {
      const cur = byStorage.get(a.storageName);
      if (cur) {
        cur.alerts++;
      } else {
        byStorage.set(a.storageName, {
          name:     a.storageName,
          alerts:   1,
          location: a.location,
          vendor:   a.vendor,
        });
      }
    }
    return [...byStorage.values()].sort((a, b) => b.alerts - a.alerts);
  }, [dashSnap, filteredAlerts, selectedVendor]);

  // Critical-only feed for the "Recent Critical Alerts" card. We surface
  // only the genuinely actionable severities (critical / error / warning)
  // so notice + informational chatter doesn't push real outages off the
  // visible rows. Already newest-first because `filteredAlerts` inherits
  // the App-level sort.
  const recentCriticalAlerts = useMemo(
    () => filteredAlerts.filter((a) => {
      const sev = String(a.severity ?? '').toLowerCase();
      return sev === 'critical' || sev === 'error' || sev === 'warning';
    }),
    [filteredAlerts],
  );

  // Trend + spark: prefer the InfluxDB-aggregated buckets when the active
  // pill is "total" (and the snapshot has landed); otherwise fall back to
  // the client-side bucketing of the vendor-filtered stream so vendor
  // switching is instant without a re-query.
  const trendBuckets = useMemo(() => {
    if (selectedVendor === 'total' && dashSnap && dashSnap.trend.length > 0) {
      return dashSnap.trend;
    }
    const { endMs, durationMs, bucketCount } = rangeWindowMs(range);
    return bucketAlerts(filteredAlerts, endMs, durationMs, bucketCount);
  }, [dashSnap, filteredAlerts, range, selectedVendor]);

  const sparkData = useMemo(
    () => trendBuckets.map(b => ({ ts: b.ts, value: b.value })),
    [trendBuckets],
  );

  const trendData = useMemo(
    () => trendBuckets.map(b => ({ ts: b.ts, value: b.value, label: b.label })),
    [trendBuckets],
  );

  const totalsForRange = useMemo(() => {
    // Total comes straight from the Influx scalar count for "total" view;
    // for vendor-filtered views it's derived from the in-range filtered
    // alerts. Delta compares the newer half of the trend buckets vs the
    // older half so the indicator reflects in-range trend direction.
    const total = (selectedVendor === 'total' && dashSnap)
      ? dashSnap.total
      : filteredAlerts.length;
    const mid   = Math.floor(trendBuckets.length / 2);
    const older = trendBuckets.slice(0, mid).reduce((a, b) => a + b.value, 0);
    const newer = trendBuckets.slice(mid).reduce((a, b) => a + b.value, 0);
    return { total, delta: newer - older };
  }, [dashSnap, filteredAlerts.length, selectedVendor, trendBuckets]);

  const ntpAlertCount = useMemo(
    () => filteredAlerts.filter(a => a.category === 'NTP').length,
    [filteredAlerts],
  );

  // Derive per-vendor counts + liveness for the System Health Overview cards.
  // dataActive is now driven by the heartbeat pipeline: a vendor's card flips
  // to the "negative" / no-data-feed visual when NO listener for that vendor
  // is reporting up across any of the three sites.
  const healthVendors = useMemo(() => {
    const buckets: Record<string, { count: number; critical: number; warning: number }> = {
      hitachi: { count: 0, critical: 0, warning: 0 },
      netapp:  { count: 0, critical: 0, warning: 0 },
      dell:    { count: 0, critical: 0, warning: 0 },
      brocade: { count: 0, critical: 0, warning: 0 },
    };
    // SHO honors location + range (mirrors what every other card shows),
    // but ignores the vendor pill since each card is its own vendor view.
    for (const a of rangeAlerts) {
      const b = buckets[a.vendor];
      if (!b) continue;
      b.count++;
      if (a.severity === 'critical') b.critical++;
      if (a.severity === 'warning')  b.warning++;
    }

    // Liveness derived from /api/health/listeners. Only trust it once the
    // first response has arrived (listeners.length > 0); otherwise fall back
    // to the hard-coded vendor.dataActive so the page doesn't briefly flash
    // every card as "offline" while we're booting.
    //
    // A vendor card is treated as "live" (dataActive=true) if EITHER:
    //   - at least one listener is `up`, OR
    //   - at least one listener is `infra_down` (status genuinely unknown
    //     because the heartbeat store is down; flipping the card to "negative"
    //     would be a misleading diagnostic — the infrastructure-down modal
    //     and the dedicated red warning band convey the actual issue).
    // A vendor goes "negative" only when EVERY listener is positively `down`
    // (the heartbeat store is reachable but no heartbeats arrive).
    const haveListenerData = listenerHealth.listeners.length > 0;
    const liveByVendor: Record<string, boolean> = {
      hitachi: false, brocade: false, netapp: false, dell: false,
    };
    for (const l of listenerHealth.listeners) {
      if (l.state === 'up' || l.state === 'infra_down') {
        liveByVendor[l.oem.toLowerCase()] = true;
      }
    }
    const anyVendorLive = Object.values(liveByVendor).some(Boolean);

    return HEALTH_VENDORS.map(v => {
      let row;
      if (v.key === 'total') {
        row = {
          ...v,
          count:    rangeAlerts.length,
          critical: rangeAlerts.filter(a => a.severity === 'critical').length,
          warning:  rangeAlerts.filter(a => a.severity === 'warning').length,
          dataActive: haveListenerData ? anyVendorLive : v.dataActive,
        };
      } else {
        const b = buckets[v.key];
        row = b
          ? { ...v, count: b.count, critical: b.critical, warning: b.warning }
          : v;
        if (haveListenerData) {
          row = { ...row, dataActive: liveByVendor[v.key] ?? false };
        }
      }
      return row;
    });
  }, [rangeAlerts, listenerHealth.listeners]);

  // pushToast / dismissToast / triggerBlink / openModal / closeModal
  // are all zustand actions selected at the top of App(). Keep only the
  // glue that orchestrates them.
  const onRefresh = () => {
    setRefreshing(true);
    setStatus('fetching');
    window.setTimeout(() => {
      setRefreshing(false);
      setStatus('live');
      pushToast({
        severity: 'informational',
        title: 'Dashboard refreshed',
        message: `Latest data from ${locations.join(' + ')} loaded`,
      });
    }, 900);
  };

  return (
    <div
      className="app"
      data-vendor={selectedVendor}
      data-vendor-offline={vendorActive ? 'false' : 'true'}
    >
      <Header
        status={status}
        selectedLocations={locations}
        allLocations={ALL_LOCATIONS}
        onLocationsChange={setLocations}
        range={range}
        onRangeChange={setRange}
        refreshing={refreshing}
        onRefresh={onRefresh}
        services={servicesForPill}
      />

      <SystemHealthOverview
        vendors={healthVendors}
        selected={selectedVendor}
        onSelect={setSelectedVendor}
        downListenersByVendor={downListenersByVendor}
        infraDownListenersByVendor={infraDownListenersByVendor}
        loading={dashLoading}
      />

      {!vendorActive && vendorName && (
        <div className="no-data-banner" role="status">
          <span className="no-data-banner__dot" />
          <strong>{vendorName}</strong>
          <span> listener is not configured — no data is being received. Select a different vendor to view live metrics.</span>
        </div>
      )}

      <div className="dashboard">
          <TotalAlertsCard
            className="area-total"
            total={totalsForRange.total}
            delta={totalsForRange.delta}
            rangeLabel={label}
            spark={sparkData}
            loading={dashLoading}
          />

          <AlertSeverityCard
            className="area-severity"
            data={severityData}
            onView={() => openModal(null)}
            onSliceClick={(r) => openModal({ severity: r.key })}
            loading={dashLoading}
          />

          <AlertTrendCard
            className="area-trend"
            data={trendData}
            rangeLabel={label}
            loading={dashLoading}
          />

          <TopSystemsCard
            className="area-systems"
            systems={filteredSystems}
            selectedLocations={locations}
            allLocations={ALL_LOCATIONS}
            onLocationsChange={setLocations}
            onView={() => openModal(null)}
            onSystemClick={(s) => openModal({ storage: s.name })}
            loading={dashLoading}
          />

          <AlertTypeBreakdownCard
            className="area-type"
            data={alertTypeData}
            rangeLabel={label}
            onView={() => openModal(null)}
            onSliceClick={(r) => openModal({ category: r.name })}
            loading={dashLoading}
          />

          <div className="area-bottom bottom-bar">
            <RecentCriticalAlertsCard
              key={`recent-critical-${selectedVendor}`}
              alerts={recentCriticalAlerts}
              rangeLabel={label}
              loading={dashLoading}
              onView={() => openModal(null)}
            />
            <NTPCard
              alertCount={ntpAlertCount}
              rangeLabel={label}
              onView={() => openModal({ category: 'NTP' })}
              loading={dashLoading}
            />
          </div>
      </div>

      <AlertDetailsModal
        key={`modal-${modalOpen ? JSON.stringify(modalFilters ?? {}) : 'closed'}`}
        open={modalOpen}
        rangeLabel={label}
        filters={modalFilters}
        alerts={filteredAlerts}
        onClose={closeModal}
      />

      <ToastStack toasts={toasts} onDismiss={dismissToast} />
      <ScreenBlink triggerKey={blinkKey} color={blinkColor} />
      <ReportPanel />

      {activeInfraModal && (
        <InfrastructureDownModal
          event={activeInfraModal}
          remaining={remainingInInfraQueue}
          onAcknowledge={acknowledgeActiveInfra}
        />
      )}
      {activeListenerModal && (
        <ListenerDownModal
          event={activeListenerModal}
          remaining={remainingInModalQueue}
          onAcknowledge={acknowledgeActiveModal}
        />
      )}
      {/*
        WS-connection-lost modal — appears only when BOTH the alerts and
        listener-health sockets have been non-`open` for longer than the
        grace window (30 s). One socket flapping is treated as transient.
      */}
      <ConnectionLostModal
        alertsStatus={alertsStatus}
        listenerHealthStatus={lhStatus}
        alertsStatusSince={alertsStatusSince}
        listenerStatusSince={lhStatusSince}
      />
    </div>
  );
}
