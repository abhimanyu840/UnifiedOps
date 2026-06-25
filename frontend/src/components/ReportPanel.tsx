import { useEffect, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { RANGE_OPTIONS, HEALTH_VENDORS } from '../data/config';
import { Calendar, ChevronDown, Check, X, FileText, Download, Clock, Loader2, MapPin } from 'lucide-react';
import type { Location, RangeKey } from '../types';
import { ClockIcon, ChevronDownIcon, LocationIcon } from './icons/Icons';

const ALL_LOCATIONS: Location[] = ['CDVL', 'BCP', 'SIFY'];
const OEM_LIST = HEALTH_VENDORS.filter(v => v.key !== 'total');

const LOC_COLORS: Record<Location, string> = {
  CDVL: 'var(--loc-cdvl)',
  BCP:  'var(--loc-bcp)',
  SIFY: 'var(--loc-sify)',
};

const toLocalInput = (d: Date): string => {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`;
};

export function ReportPanel() {
  const [isOpen, setIsOpen]         = useState(false);
  const [reportType, setReportType] = useState<'hardware' | 'health_check'>('hardware');

  /* ---- Time Range ---- */
  const [rangeMode, setRangeMode]   = useState<'relative' | 'custom'>('relative');
  const [rangeKey, setRangeKey]     = useState<RangeKey>('1d');
  const [rangeOpen, setRangeOpen]   = useState(false);
  const [showCustom, setShowCustom] = useState(false);
  const rangeRef = useRef<HTMLDivElement>(null);

  const now       = new Date();
  const yesterday = new Date(now.getTime() - 86_400_000);
  const [customStart, setCustomStart] = useState(toLocalInput(yesterday));
  const [customStop, setCustomStop]   = useState(toLocalInput(now));
  const [customErr, setCustomErr]     = useState<string | null>(null);

  /* ---- Locations ---- */
  const [selectedSites, setSelectedSites] = useState<Location[]>([...ALL_LOCATIONS]);
  const [locOpen, setLocOpen]             = useState(false);
  const locRef = useRef<HTMLDivElement>(null);

  /* ---- OEMs ---- */
  const [selectedVendors, setSelectedVendors] = useState<string[]>(OEM_LIST.map(v => v.key));
  const [oemOpen, setOemOpen]                 = useState(false);
  const oemRef = useRef<HTMLDivElement>(null);

  /* ---- Format ---- */
  const [format, setFormat] = useState<'csv'|'xlsx'|'pdf'>('csv');
  const [formatOpen, setFormatOpen] = useState(false);
  const formatRef = useRef<HTMLDivElement>(null);

  const [isDownloading, setIsDownloading] = useState(false);

  /* Close dropdowns on outside click */
  useEffect(() => {
    const onDown = (e: MouseEvent) => {
      if (rangeRef.current && !rangeRef.current.contains(e.target as Node)) {
        setRangeOpen(false);
        setShowCustom(false);
        setCustomErr(null);
      }
      if (locRef.current && !locRef.current.contains(e.target as Node)) {
        setLocOpen(false);
      }
      if (oemRef.current && !oemRef.current.contains(e.target as Node)) {
        setOemOpen(false);
      }
      if (formatRef.current && !formatRef.current.contains(e.target as Node)) {
        setFormatOpen(false);
      }
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, []);

  /* ---- Togglers ---- */
  const toggleSite = (loc: Location) => {
    if (selectedSites.includes(loc)) {
      if (selectedSites.length === 1) return;
      setSelectedSites(selectedSites.filter(s => s !== loc));
    } else {
      setSelectedSites(ALL_LOCATIONS.filter(s => selectedSites.includes(s) || s === loc));
    }
  };

  const toggleVendor = (key: string) => {
    if (selectedVendors.includes(key)) {
      if (selectedVendors.length === 1) return;
      setSelectedVendors(selectedVendors.filter(v => v !== key));
    } else {
      setSelectedVendors(OEM_LIST.map(v => v.key).filter(k => selectedVendors.includes(k) || k === key));
    }
  };

  const applyCustom = () => {
    const a = new Date(customStart).getTime();
    const b = new Date(customStop).getTime();
    if (Number.isNaN(a) || Number.isNaN(b)) {
      setCustomErr('Pick both start and end times.');
      return;
    }
    if (b <= a) {
      setCustomErr('End time must be after start time.');
      return;
    }
    setCustomErr(null);
    setRangeMode('custom');
    setRangeOpen(false);
    setShowCustom(false);
  };

  /* ---- Labels ---- */
  const rangeLabel = rangeMode === 'relative'
    ? RANGE_OPTIONS.find(o => o.key === rangeKey)?.label ?? rangeKey
    : (() => {
        const fmt = (d: Date) =>
          `${d.toLocaleDateString([], { month: 'short', day: 'numeric' })} ${d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
        return `${fmt(new Date(customStart))} → ${fmt(new Date(customStop))}`;
      })();

  const allSitesSelected = selectedSites.length === ALL_LOCATIONS.length;
  const locLabel = allSitesSelected ? 'All Locations' : selectedSites.join(' + ');

  const allOemSelected = selectedVendors.length === OEM_LIST.length;
  const oemLabel = allOemSelected ? 'All OEMs' : selectedVendors.map(k => OEM_LIST.find(v => v.key === k)?.name ?? k).join(', ');

  /* ---- Download ---- */
  const canDownload = selectedSites.length > 0 && selectedVendors.length > 0;

  const handleDownload = async () => {
    if (isDownloading) return;
    setIsDownloading(true);
    try {
      const params = new URLSearchParams();
      if (rangeMode === 'relative') {
        params.append('range', rangeKey);
      } else {
        params.append('start', new Date(customStart).toISOString());
        params.append('stop', new Date(customStop).toISOString());
      }
      selectedSites.forEach(s => params.append('site', s));
      selectedVendors.forEach(v => params.append('vendor', v));
      params.append('format', format);
      params.append('report_type', reportType);

      const url = `/api/reports/download?${params.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const errText = await res.text();
        throw new Error(errText || res.statusText);
      }
      
      const blob = await res.blob();
      const downloadUrl = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = downloadUrl;
      
      let filename = `report_${reportType}_${new Date().getTime()}.${format}`;
      const disposition = res.headers.get('Content-Disposition');
      if (disposition && disposition.indexOf('attachment') !== -1) {
        const filenameRegex = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/;
        const matches = filenameRegex.exec(disposition);
        if (matches != null && matches[1]) {
          filename = matches[1].replace(/['"]/g, '');
        }
      }
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(downloadUrl);
    } catch (err: any) {
      console.error(err);
      alert(`Download failed: ${err.message}`);
    } finally {
      setIsDownloading(false);
    }
  };

  const timeoutRef = useRef<number | null>(null);

  const handleMouseEnter = () => {
    if (timeoutRef.current) window.clearTimeout(timeoutRef.current);
    setIsOpen(true);
  };

  const handleMouseLeave = () => {
    if (rangeOpen || locOpen || oemOpen || showCustom || formatOpen) return;
    timeoutRef.current = window.setTimeout(() => {
      setIsOpen(false);
    }, 400);
  };

  return (
    <>
      {/* Trigger tab */}
      <div
        className="rp-trigger"
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        onClick={handleMouseEnter}
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="m15 18-6-6 6-6" />
        </svg>
      </div>

      <AnimatePresence>
        {isOpen && (
          <>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="rp-backdrop"
              onClick={() => setIsOpen(false)}
            />

            <motion.div
              initial={{ x: '100%' }}
              animate={{ x: 0 }}
              exit={{ x: '100%' }}
              transition={{ type: 'spring', damping: 26, stiffness: 220 }}
              className="rp-panel"
              onMouseEnter={handleMouseEnter}
              onMouseLeave={handleMouseLeave}
            >
              {/* ---- Header ---- */}
              <div className="rp-header">
                <div className="rp-header__left">
                  <div className="rp-header__icon">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none"
                      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8Z" />
                      <path d="M14 2v6h6" />
                      <path d="M16 13H8" />
                      <path d="M16 17H8" />
                      <path d="M10 9H8" />
                    </svg>
                  </div>
                  <div>
                    <h2 className="rp-header__title">Export Reports</h2>
                    <p className="rp-header__sub">Download CSV alert history</p>
                  </div>
                </div>
                <button className="btn-icon" onClick={() => setIsOpen(false)}>
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
                    stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="m9 18 6-6-6-6" />
                  </svg>
                </button>
              </div>

              {/* ---- Body ---- */}
              <div className="rp-body">
                {/* Report Type Toggle */}
                <div className="rp-field">
                  <div className="seg-control" style={{ display: 'flex', background: 'var(--bg-card)', padding: '4px', borderRadius: '8px', border: '1px solid var(--border-soft)', position: 'relative' }}>
                    <button
                      type="button"
                      onClick={() => setReportType('hardware')}
                      style={{
                        flex: 1, padding: '8px 12px', fontSize: '13px', border: 'none', cursor: 'pointer',
                        background: 'transparent',
                        color: reportType === 'hardware' ? 'var(--bg-main)' : 'var(--text-dim)',
                        fontWeight: reportType === 'hardware' ? 600 : 400,
                        position: 'relative',
                        zIndex: 1,
                        transition: 'color 0.2s ease'
                      }}
                    >
                      {reportType === 'hardware' && (
                        <motion.div
                          layoutId="active-report-type"
                          style={{ position: 'absolute', inset: 0, background: 'var(--primary)', borderRadius: '6px', zIndex: -1 }}
                          transition={{ type: 'spring', stiffness: 400, damping: 30 }}
                        />
                      )}
                      Hardware Alerts
                    </button>
                    <button
                      type="button"
                      onClick={() => setReportType('health_check')}
                      style={{
                        flex: 1, padding: '8px 12px', fontSize: '13px', border: 'none', cursor: 'pointer',
                        background: 'transparent',
                        color: reportType === 'health_check' ? 'var(--bg-main)' : 'var(--text-dim)',
                        fontWeight: reportType === 'health_check' ? 600 : 400,
                        position: 'relative',
                        zIndex: 1,
                        transition: 'color 0.2s ease'
                      }}
                    >
                      {reportType === 'health_check' && (
                        <motion.div
                          layoutId="active-report-type"
                          style={{ position: 'absolute', inset: 0, background: 'var(--primary)', borderRadius: '6px', zIndex: -1 }}
                          transition={{ type: 'spring', stiffness: 400, damping: 30 }}
                        />
                      )}
                      Health Check Report
                    </button>
                  </div>
                </div>

                {/* Time Range */}
                <div className="rp-field">
                  <span className="rp-field__label">Time Range</span>
                  <div className="dropdown" ref={rangeRef}>
                    <button
                      type="button"
                      className="btn-pill rp-field__pill"
                      onClick={() => { setRangeOpen(v => !v); setShowCustom(false); setCustomErr(null); }}
                    >
                      <ClockIcon size={14} />
                      <span>{rangeLabel}</span>
                      <ChevronDownIcon size={14} className={rangeOpen ? 'chev chev--open' : 'chev'} />
                    </button>

                    {rangeOpen && !showCustom && (
                      <div className="dropdown-menu rp-dropdown rp-dropdown--range" role="menu">
                        <div className="rp-dropdown__scroll">
                          {RANGE_OPTIONS.map(o => {
                            const active = rangeMode === 'relative' && rangeKey === o.key;
                            return (
                              <button
                                type="button"
                                key={o.key}
                                className={`dropdown-item ${active ? 'is-checked' : ''}`}
                                onClick={() => {
                                  setRangeKey(o.key);
                                  setRangeMode('relative');
                                  setRangeOpen(false);
                                }}
                              >
                                <span className="check-box">{active ? '✓' : ''}</span>
                                <span>{o.label}</span>
                              </button>
                            );
                          })}
                        </div>
                        <div className="dropdown-divider" />
                        <button
                          type="button"
                          className={`dropdown-item ${rangeMode === 'custom' ? 'is-checked' : ''}`}
                          onClick={() => setShowCustom(true)}
                        >
                          <span className="check-box">{rangeMode === 'custom' ? '✓' : ''}</span>
                          <span>Custom range…</span>
                        </button>
                      </div>
                    )}

                    {rangeOpen && showCustom && (
                      <div className="dropdown-menu dropdown-menu--wide rp-dropdown" role="menu">
                        <div className="dropdown-heading">Custom Range</div>
                        <label className="custom-field">
                          <span className="custom-field__label">From</span>
                          <input
                            type="datetime-local"
                            className="custom-field__input"
                            value={customStart}
                            onChange={(e) => setCustomStart(e.target.value)}
                          />
                        </label>
                        <label className="custom-field">
                          <span className="custom-field__label">To</span>
                          <input
                            type="datetime-local"
                            className="custom-field__input"
                            value={customStop}
                            onChange={(e) => setCustomStop(e.target.value)}
                          />
                        </label>
                        {customErr && <div className="custom-error">{customErr}</div>}
                        <div className="custom-actions">
                          <button
                            type="button"
                            className="btn-secondary"
                            onClick={() => { setShowCustom(false); setCustomErr(null); }}
                          >
                            Back
                          </button>
                          <button type="button" className="btn-primary" onClick={applyCustom}>
                            Apply
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                </div>

                {/* Locations */}
                <div className="rp-field">
                  <span className="rp-field__label">Locations</span>
                  <div className="dropdown" ref={locRef}>
                    <button
                      type="button"
                      className="btn-pill rp-field__pill"
                      onClick={() => setLocOpen(v => !v)}
                    >
                      <LocationIcon size={14} />
                      <span>{locLabel}</span>
                      <ChevronDownIcon size={14} className={locOpen ? 'chev chev--open' : 'chev'} />
                    </button>

                    {locOpen && (
                      <div className="dropdown-menu rp-dropdown" role="menu">
                        <button
                          type="button"
                          className={`dropdown-item ${allSitesSelected ? 'is-checked' : ''}`}
                          onClick={() => setSelectedSites([...ALL_LOCATIONS])}
                        >
                          <span className="check-box">{allSitesSelected ? '✓' : ''}</span>
                          <span>All Locations</span>
                        </button>
                        <div className="dropdown-divider" />
                        {ALL_LOCATIONS.map(loc => {
                          const checked = selectedSites.includes(loc);
                          return (
                            <button
                              type="button"
                              key={loc}
                              className={`dropdown-item ${checked ? 'is-checked' : ''}`}
                              onClick={() => toggleSite(loc)}
                            >
                              <span className="check-box">{checked ? '✓' : ''}</span>
                              <span className="dot" style={{ background: LOC_COLORS[loc] }} />
                              <span>{loc}</span>
                            </button>
                          );
                        })}
                      </div>
                    )}
                  </div>
                </div>

                {/* OEMs */}
                <div className="rp-field">
                  <span className="rp-field__label">Storage / OEMs</span>
                  <div className="dropdown" ref={oemRef}>
                    <button
                      type="button"
                      className="btn-pill rp-field__pill"
                      onClick={() => setOemOpen(v => !v)}
                    >
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
                        stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <rect x="2" y="2" width="20" height="8" rx="2" ry="2" />
                        <rect x="2" y="14" width="20" height="8" rx="2" ry="2" />
                        <circle cx="6" cy="6" r="0" fill="currentColor"><animate attributeName="r" values="0;1;0" dur="0s" /></circle>
                        <line x1="6" y1="6" x2="6.01" y2="6" />
                        <line x1="6" y1="18" x2="6.01" y2="18" />
                      </svg>
                      <span>{oemLabel}</span>
                      <ChevronDownIcon size={14} className={oemOpen ? 'chev chev--open' : 'chev'} />
                    </button>

                    {oemOpen && (
                      <div className="dropdown-menu rp-dropdown" role="menu">
                        <button
                          type="button"
                          className={`dropdown-item ${allOemSelected ? 'is-checked' : ''}`}
                          onClick={() => setSelectedVendors(OEM_LIST.map(v => v.key))}
                        >
                          <span className="check-box">{allOemSelected ? '✓' : ''}</span>
                          <span>All OEMs</span>
                        </button>
                        <div className="dropdown-divider" />
                        {OEM_LIST.map(v => {
                          const checked = selectedVendors.includes(v.key);
                          return (
                            <button
                              type="button"
                              key={v.key}
                              className={`dropdown-item ${checked ? 'is-checked' : ''}`}
                              onClick={() => toggleVendor(v.key)}
                            >
                              <span className="check-box">{checked ? '✓' : ''}</span>
                              <span className="dot" style={{ background: v.iconBg }} />
                              <span>{v.name}</span>
                            </button>
                          );
                        })}
                      </div>
                    )}
                  </div>
                </div>

                {/* Format */}
                <div className="rp-field">
                  <span className="rp-field__label">File Format</span>
                  <div className="dropdown" ref={formatRef}>
                    <button
                      type="button"
                      className="btn-pill rp-field__pill"
                      onClick={() => setFormatOpen(v => !v)}
                    >
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8Z" />
                        <path d="M14 2v6h6" />
                        <path d="M16 13H8" />
                        <path d="M16 17H8" />
                        <path d="M10 9H8" />
                      </svg>
                      <span>{format.toUpperCase()}</span>
                      <ChevronDownIcon size={14} className={formatOpen ? 'chev chev--open' : 'chev'} />
                    </button>

                    {formatOpen && (
                      <div className="dropdown-menu rp-dropdown" role="menu">
                        {(['csv', 'xlsx', 'pdf'] as const).map(f => (
                          <button
                            type="button"
                            key={f}
                            className={`dropdown-item ${format === f ? 'is-checked' : ''}`}
                            onClick={() => { setFormat(f); setFormatOpen(false); }}
                          >
                            <span className="check-box">{format === f ? '✓' : ''}</span>
                            <span>{f.toUpperCase()}</span>
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* ---- Footer ---- */}
              <div className="rp-footer">
                <button
                  className={`rp-download ${!canDownload || isDownloading ? 'rp-download--disabled' : ''}`}
                  onClick={handleDownload}
                  disabled={!canDownload || isDownloading}
                >
                  {isDownloading ? (
                    <Loader2 size={16} className="animate-spin" />
                  ) : (
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
                      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                      <polyline points="7 10 12 15 17 10" />
                      <line x1="12" y1="15" x2="12" y2="3" />
                    </svg>
                  )}
                  <span>{isDownloading ? 'Downloading...' : canDownload ? `Download ${format.toUpperCase()} ${reportType === 'health_check' ? 'Health Check' : 'Alerts'}` : 'Select filters'}</span>
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </>
  );
}
