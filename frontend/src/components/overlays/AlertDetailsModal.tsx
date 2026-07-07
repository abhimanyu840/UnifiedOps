import { useEffect, useMemo, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import type { Location, RecentAlert, Severity, Vendor } from '../../types';
import { FilterSelect } from './FilterSelect';
import { AlertsDataTable } from '../tables/AlertsDataTable';
import { bestCategory } from '../../utils/category';

export interface ModalFilters {
  severity?: Severity;
  category?: string;
  storage?: string;
  vendor?:  Vendor;
  location?: Location;
}

interface Props {
  open: boolean;
  rangeLabel: string;
  filters: ModalFilters | null;
  alerts: RecentAlert[];
  onClose: () => void;
}

const SEVERITIES: Severity[] = ['critical', 'error', 'warning', 'notice', 'informational'];
const VENDORS:    Vendor[]   = ['hitachi', 'brocade', 'netapp', 'dell'];
const LOCATIONS:  Location[] = ['CDVL', 'BCP', 'SIFY'];

interface LiveFilters {
  severity: '' | Severity;
  vendor:   '' | Vendor;
  location: '' | Location;
  category: string;     // '' = all
  storage:  string;     // '' = all
  search:   string;     // free-text search over event/storage/ip
}

const emptyLive = (): LiveFilters => ({
  severity: '', vendor: '', location: '', category: '', storage: '', search: '',
});

const fromIncoming = (f: ModalFilters | null): LiveFilters => ({
  severity: f?.severity ?? '',
  vendor:   f?.vendor   ?? '',
  location: f?.location ?? '',
  category: f?.category ?? '',
  storage:  f?.storage  ?? '',
  search:   '',
});

const eqi = (a: unknown, b: unknown): boolean =>
  String(a ?? '').toLowerCase() === String(b ?? '').toLowerCase();

const matches = (a: RecentAlert, f: LiveFilters): boolean => {
  if (f.severity && !eqi(a.severity, f.severity)) return false;
  if (f.vendor   && !eqi(a.vendor,   f.vendor))   return false;
  if (f.location && !eqi(a.location, f.location)) return false;
  if (f.category) {
    // The Alert Type Breakdown donut hands us a DISPLAY category
    // ("Disk failure") whereas alert.category holds the raw listener
    // tag ("disk_failure"). Normalise both sides through bestCategory
    // so slice-click filters land on the right rows. We also accept an
    // exact raw match so existing manual dropdown picks still work.
    const aDisplay = bestCategory(a.category, a.event);
    if (aDisplay !== f.category && a.category !== f.category) return false;
  }
  if (f.storage  && a.storageName   !== f.storage)  return false;
  if (f.search) {
    const q = f.search.toLowerCase();
    const hay = `${a.storageName} ${a.ip} ${a.event} ${a.category} ${bestCategory(a.category, a.event)}`.toLowerCase();
    if (!hay.includes(q)) return false;
  }
  return true;
};

const titleCase = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

export function AlertDetailsModal({
  open, rangeLabel, filters, alerts, onClose,
}: Props) {
  const [live, setLive] = useState<LiveFilters>(() => fromIncoming(filters));

  const updateField = <K extends keyof LiveFilters>(key: K, val: LiveFilters[K]) => {
    setLive(prev => ({ ...prev, [key]: val }));
  };

  // Reset filters whenever the modal is reopened with a new incoming context.
  useEffect(() => {
    if (open) setLive(fromIncoming(filters));
  }, [open, filters]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [open, onClose]);

  // Distinct categories + storages from the *unfiltered* dataset so the
  // dropdowns stay stable even as the user narrows results.
  const categoryOptions = useMemo(
    () => Array.from(new Set(alerts.map(a => a.category))).sort(),
    [alerts],
  );
  const storageOptions = useMemo(
    () => Array.from(new Set(alerts.map(a => a.storageName))).sort(),
    [alerts],
  );

  const filtered = useMemo(
    () => alerts.filter(a => matches(a, live)),
    [alerts, live],
  );

  const activeCount = useMemo(() => {
    let count = 0;
    if (live.severity) count++;
    if (live.vendor) count++;
    if (live.location) count++;
    if (live.category) count++;
    if (live.storage) count++;
    if (live.search) count++;
    return count;
  }, [live]);

  return (
    <AnimatePresence>
      {open && (
        <motion.div 
          className="modal" 
          role="dialog" 
          aria-modal="true" 
          onClick={onClose}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.3 }}
        >
          <motion.div 
            className="modal__card" 
            onClick={(e) => e.stopPropagation()}
            initial={{ scale: 0.95, opacity: 0, y: 10 }}
            animate={{ scale: 1, opacity: 1, y: 0 }}
            exit={{ scale: 0.95, opacity: 0, y: 10 }}
            transition={{ duration: 0.25, ease: "easeOut" }}
          >
            <div className="modal__head">
              <div>
                <span className="modal__title">Alert Details</span>
                <span className="modal__sub">{rangeLabel} · {filtered.length} matching</span>
              </div>
              <button
                type="button"
                className="modal__close"
                aria-label="Close"
                onClick={onClose}
              >
                ×
              </button>
            </div>

            <div className="modal__filters">
              <FilterSelect
                label="Severity"
                value={live.severity}
                onChange={(v) => updateField('severity', v as Severity | '')}
                options={SEVERITIES.map(s => ({ value: s, label: titleCase(s) }))}
                width={130}
              />

              <FilterSelect
                label="Vendor"
                value={live.vendor}
                onChange={(v) => updateField('vendor', v as Vendor | '')}
                options={VENDORS.map(v => ({ value: v, label: titleCase(v) }))}
                width={120}
              />

              <FilterSelect
                label="Location"
                value={live.location}
                onChange={(v) => updateField('location', v as Location | '')}
                options={LOCATIONS.map(l => ({ value: l, label: l }))}
                width={110}
              />

              <FilterSelect
                label="Category"
                value={live.category}
                onChange={(v) => updateField('category', v)}
                options={categoryOptions.map(c => ({ value: c, label: c }))}
                searchable
                width={170}
              />

              <FilterSelect
                label="Storage"
                value={live.storage}
                onChange={(v) => updateField('storage', v)}
                options={storageOptions.map(s => ({ value: s, label: s }))}
                searchable
                width={200}
              />

              <div className="modal-filter modal-filter--grow">
                <label className="modal-filter__label">Search</label>
                <input
                  type="text"
                  className="modal-filter__input"
                  placeholder="storage / IP / event text"
                  value={live.search}
                  onChange={(e) => updateField('search', e.target.value)}
                />
              </div>

              <button
                type="button"
                className="modal-filter__clear"
                onClick={() => setLive(emptyLive())}
                disabled={activeCount === 0}
                title="Reset all filters"
              >
                Clear {activeCount > 0 ? `(${activeCount})` : ''}
              </button>
            </div>

            <div className="modal__body">
              <AlertsDataTable
                key={JSON.stringify(live)}
                alerts={filtered}
                variant="full"
                infinite
                pageSize={40}
                emptyText={
                  activeCount > 0
                    ? 'No matching alerts (filters active)'
                    : `No matching alerts in ${rangeLabel.toLowerCase()}`
                }
              />
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
