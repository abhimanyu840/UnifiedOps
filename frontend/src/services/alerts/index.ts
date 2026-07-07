/**
 * alerts microservice — public API.
 *
 * Owns everything related to alert ingestion (real data from InfluxDB via
 * `useRecentAlerts`), the alert-detail modal, and every dashboard card
 * whose primary axis is "alerts in the active range":
 *
 *   - Total Alerts (number + sparkline)
 *   - Alert Severity (donut)
 *   - Alert Trend (area chart)
 *   - Alert Type Breakdown (donut)
 *   - Top Systems by Alerts (bars)
 *   - Recent Critical Alerts (table)
 *   - Alert Details modal (full table with multi-dim filters)
 *
 * Internal modules may import from each other freely; consumers outside
 * this service must go through this barrel.
 */
export { TotalAlertsCard }             from '../../components/cards/TotalAlertsCard';
export { AlertSeverityCard }           from '../../components/cards/AlertSeverityCard';
export { AlertTrendCard }              from '../../components/cards/AlertTrendCard';
export { TopSystemsCard }              from '../../components/cards/TopSystemsCard';
export { AlertTypeBreakdownCard }      from '../../components/cards/AlertTypeBreakdownCard';
export { RecentAlertsCard }            from '../../components/cards/RecentAlertsCard';
export { AlertDetailsModal }           from '../../components/overlays/AlertDetailsModal';
export type { ModalFilters }           from '../../components/overlays/AlertDetailsModal';
export { AlertRowHoverCard }           from '../../components/hover/AlertRowHoverCard';
export { useAlertRowHover }            from '../../hooks/useAlertRowHover';
export { useAlertsStore }              from '../../stores/useAlertsStore';
export type { BucketStatus }           from '../../stores/useAlertsStore';
export { useDashboardStore }           from '../../stores/useDashboardStore';
export type {
  DashboardSnapshot,
  TrendBucket,
  TopSystemRow,
  SubscriptionParams,
}                                       from '../../stores/useDashboardStore';
export { useFilterStore }              from '../../stores/useFilterStore';
export { useModalStore }               from '../../stores/useModalStore';
export { useUiStore }                  from '../../stores/useUiStore';
