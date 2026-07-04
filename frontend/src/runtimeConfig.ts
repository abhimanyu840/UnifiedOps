/**
 * Runtime configuration reader.
 *
 * The backend serves `/runtime-config.js` from env vars and injects a
 * `<script>` tag into index.html that sets `window.__UNIFIEDOPS_CONFIG__`
 * BEFORE the React bundle runs. This module reads from there with typed
 * defaults so the rest of the app never has to touch `window` directly.
 *
 * To add a new tunable:
 *   1. Add `UNIFIEDOPS_PUBLIC_FOO` to deploy/unifiedops-ui.env.example
 *      and to server.py's `_PUBLIC_DEFAULTS` map (for an inline default).
 *   2. Read it here via `getString('FOO', 'fallback')`.
 *
 * Same dist, any deployment — just change the env file.
 */

interface RuntimeConfigShape {
  [key: string]: string | undefined;
}

declare global {
  interface Window {
    __UNIFIEDOPS_CONFIG__?: RuntimeConfigShape;
  }
}

const raw: RuntimeConfigShape =
  (typeof window !== 'undefined' && window.__UNIFIEDOPS_CONFIG__) || {};

const getString = (key: string, fallback = ''): string => {
  const v = raw[key];
  return typeof v === 'string' && v.length > 0 ? v : fallback;
};

const getList = (key: string, fallback: string[] = []): string[] => {
  const v = getString(key, '');
  if (!v) return fallback;
  return v
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
};

export const runtimeConfig = Object.freeze({
  /** Top-bar product / customer brand text */
  brandTitle:        getString('BRAND_TITLE',       'UnifiedOps'),

  /** Browser tab title */
  dashboardTitle:    getString('DASHBOARD_TITLE',   'UnifiedOps v2'),
  /** Default time range key (`5m`, `15m`, `30m`, `1h` … `30d`) */
  defaultRange:      getString('DEFAULT_RANGE',     '6h'),
  /** Override the API base URL — empty = same origin */
  apiBase:           getString('API_BASE',          ''),
  /** Override the WS base URL — empty = same origin (auto ws:// vs wss://) */
  wsBase:            getString('WS_BASE',           ''),
  /** Enabled site list */
  sites:             getList  ('SITES',             ['CDVL', 'BCP', 'SIFY']),
  /** Enabled vendor list */
  vendors:           getList  ('VENDORS',           ['hitachi', 'brocade', 'netapp', 'dell']),
  /** Free-form refresh-hint text shown next to LIVE pill */
  refreshHintText:   getString('REFRESH_HINT_TEXT', 'Live · 5s'),
  /**
   * Raw access — for any UNIFIEDOPS_PUBLIC_FOO env var you add later
   * without bumping this file. Returns '' when unset.
   */
  raw: (key: string): string => getString(key, ''),
});

export type RuntimeConfig = typeof runtimeConfig;
