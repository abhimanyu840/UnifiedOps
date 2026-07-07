/**
 * Maps raw `trap_category` strings (whatever the per-vendor listener
 * decides to tag) into one of the 12 display buckets shown by the
 * Alert Type Breakdown card.
 *
 * Anything that doesn't match goes to "Others", so adding new
 * listener-side categories never silently disappears from the dashboard.
 *
 * If a listener writes a vendor-specific snippet inside the message
 * body (Hitachi RefCode prefixes, FOS event types, etc.) and that
 * substring matches one of the keywords below, we still bucket it
 * correctly — see `categorizeFromEvent` for the body-text path.
 */
export const DISPLAY_CATEGORIES = [
  'Disk failure',
  'I/O pool threshold',
  'SUM detection',
  'Communication error',
  'Performance disordered',
  'Replication error',
  'Hardware error',
  'SFP/SFP+ issue',
  'Fan/PSU issue',
  'License expired',
  'NTP',
  'Others',
] as const;

export type DisplayCategory = (typeof DISPLAY_CATEGORIES)[number];

// Raw -> display map. Listener-side category names typically use
// lower_snake_case; keep the keys lowercase for case-insensitive lookup.
const RAW_TO_DISPLAY: Record<string, DisplayCategory> = {
  // Disk / drive
  disk_failure:          'Disk failure',
  drive_failure:         'Disk failure',
  raid_degraded:         'Disk failure',
  drive_temp_error:      'Disk failure',
  drive_port_error:      'Disk failure',
  pdev_erase:            'Disk failure',
  media_sanitization:    'Disk failure',
  dynamic_sparing:       'Disk failure',
  correction_copy:       'Disk failure',

  // Pool / capacity
  pool_utilization:      'I/O pool threshold',
  dp_pool_threshold:     'I/O pool threshold',
  dp_pool_full:          'I/O pool threshold',
  pool_error:            'I/O pool threshold',
  pool_threshold:        'I/O pool threshold',
  qos_alert:             'I/O pool threshold',
  ldev_blockade:         'I/O pool threshold',
  volume_alert:          'I/O pool threshold',

  // Firmware / config drift
  sum_detection:         'SUM detection',
  firmware_alert:        'SUM detection',
  config_change:         'SUM detection',
  audit_log:             'SUM detection',
  gum_alert:             'SUM detection',
  boot_error:            'SUM detection',
  format_complete:       'SUM detection',

  // Communication
  comm_error:            'Communication error',
  communication_error:   'Communication error',
  link_down:             'Communication error',
  pci_error:             'Communication error',

  // Performance
  performance_disordered:'Performance disordered',
  cache_alert:           'Performance disordered',
  shared_memory_alert:   'Performance disordered',
  tier_relocation:       'Performance disordered',
  external_storage:      'Performance disordered',

  // Replication
  replication_alert:     'Replication error',
  replication_error:     'Replication error',
  replication_pair_suspend:'Replication error',
  replication_journal:   'Replication error',
  gad_alert:             'Replication error',
  snapshot_alert:        'Replication error',
  backup_alert:          'Replication error',

  // Hardware
  hardware_error:        'Hardware error',
  controller_fault:      'Hardware error',
  battery_alert:         'Hardware error',
  expander_alert:        'Hardware error',
  encryption_alert:      'Hardware error',
  sas_port:              'Hardware error',
  blade_event:           'Hardware error',
  alarm_led:             'Hardware error',
  env_warning:           'Hardware error',
  temperature_alarm:     'Hardware error',

  // SFP / port
  sfp_issue:             'SFP/SFP+ issue',
  port_event:            'SFP/SFP+ issue',
  port_fault:            'SFP/SFP+ issue',
  fabric_event:          'SFP/SFP+ issue',
  zoning:                'SFP/SFP+ issue',
  fabric_merge:          'SFP/SFP+ issue',
  isl_event:             'SFP/SFP+ issue',
  sannav_event:          'SFP/SFP+ issue',

  // Power / cooling
  fan_failure:           'Fan/PSU issue',
  power_failure:         'Fan/PSU issue',
  ups_alert:             'Fan/PSU issue',
  air_filter:            'Fan/PSU issue',

  // License
  license_alert:         'License expired',
  license_expired:       'License expired',

  // NTP
  ntp_alert:             'NTP',
  ntp:                   'NTP',
  time_sync:             'NTP',

  // Tests / catch-all
  test_trap:             'Others',
  auth_failure:          'Others',
  other:                 'Others',
};

// Lightweight keyword bag for body-text categorization (used when the
// listener doesn't set trap_category and we have to fall back to
// regex on the event string).
const BODY_HINTS: Array<[RegExp, DisplayCategory]> = [
  [/\b(drive|disk).*(fail|degrad|spare|copy|erase|sanit)\b/i, 'Disk failure'],
  [/\b(pool|ldev|qos|volume).*(threshold|full|blockade|alert)\b/i, 'I/O pool threshold'],
  [/\b(sum|firmware|microcode|gum|audit|format)\b/i,           'SUM detection'],
  [/\b(comm|communication|link.*down|pci)\b/i,                 'Communication error'],
  [/\b(perf|performance|cache|sm full|shared.?memory|tier)\b/i,'Performance disordered'],
  [/\b(repl|replication|tc.?pair|snapshot|backup|gad|journal)\b/i, 'Replication error'],
  [/\b(controller|battery|encrypt|expander|sas|hardware|blade)\b/i, 'Hardware error'],
  [/\b(sfp|port|zone|fabric|isl|sannav)\b/i,                   'SFP/SFP+ issue'],
  [/\b(fan|psu|power|ups|cooling|filter|temperature|env)\b/i,  'Fan/PSU issue'],
  [/\blicense\b/i,                                             'License expired'],
  [/\bntp\b/i,                                                 'NTP'],
];

export function normalizeCategory(raw: string | null | undefined): DisplayCategory {
  if (!raw) return 'Others';
  const key = String(raw).trim().toLowerCase();
  if (key in RAW_TO_DISPLAY) return RAW_TO_DISPLAY[key];
  return 'Others';
}

/**
 * Best-effort body-text categorizer. Used by the alert hover-card and
 * (eventually) by the listener parser when no `trap_category` tag was
 * extracted. The match order matters — earlier patterns win.
 */
export function categorizeFromEvent(event: string | null | undefined): DisplayCategory {
  if (!event) return 'Others';
  const s = String(event);
  for (const [re, cat] of BODY_HINTS) {
    if (re.test(s)) return cat;
  }
  return 'Others';
}

/**
 * Combine the raw `trap_category` tag with body-text inference: if the
 * tag is missing / "other", look at the event body.
 */
export function bestCategory(
  rawCategory: string | null | undefined,
  event:       string | null | undefined,
): DisplayCategory {
  const fromTag = normalizeCategory(rawCategory);
  if (fromTag !== 'Others') return fromTag;
  return categorizeFromEvent(event);
}
