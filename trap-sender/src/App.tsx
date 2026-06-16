import { useCallback, useEffect, useMemo, useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import {
  AlertTriangle, CheckCircle2, Loader2, Pause, Play, Send, ShieldAlert,
  Sparkles, Wifi, WifiOff,
} from 'lucide-react';
import { Toaster, toast } from 'sonner';
import { api, type CatalogueEntry, type LocationInfo, type Vendor } from './api';

type Tab = 'manual' | 'schedule' | 'sannav';

const DEFAULT_VENDOR = 'Hitachi';

export default function App() {
  const [tab,      setTab]      = useState<Tab>('manual');
  const [vendors,  setVendors]  = useState<Vendor[]>([]);
  const [vendor,   setVendor]   = useState<string>(DEFAULT_VENDOR);
  const [location, setLocation] = useState<string>('CDVL');
  const [locations,setLocations]= useState<Record<string, LocationInfo>>({});
  const [catalog,  setCatalog]  = useState<CatalogueEntry[]>([]);
  const [refcode,  setRefcode]  = useState<string>('');
  const [severityOverride, setSeverityOverride] = useState<string>('');
  const [sourceIp, setSourceIp] = useState<string>('');
  const [textOverride, setTextOverride] = useState<string>('');
  const [randomPick, setRandomPick] = useState<boolean>(false);
  const [useGum,   setUseGum]   = useState<boolean>(true);
  const [rfc5424,  setRfc5424]  = useState<boolean>(true);
  const [sending,  setSending]  = useState<boolean>(false);
  const [backendOk, setBackendOk] = useState<boolean | null>(null);

  // ---- Initial fetch ------------------------------------------------------
  useEffect(() => {
    api.health().then(() => setBackendOk(true)).catch(() => setBackendOk(false));
    api.vendors().then(vs => {
      setVendors(vs);
      const v0 = vs.find(v => v.vendor === DEFAULT_VENDOR) ?? vs[0];
      if (v0) {
        setVendor(v0.vendor);
        setLocation(v0.locations[0] ?? 'CDVL');
      }
    }).catch(err => toast.error('Backend unreachable', { description: String(err) }));
  }, []);

  // ---- Refresh per-vendor catalog + locations whenever vendor changes ----
  useEffect(() => {
    if (!vendor) return;
    api.catalog(vendor).then(cat => {
      setCatalog(cat);
      setRefcode('');
    }).catch(err => toast.error('Catalogue load failed', { description: String(err) }));
    api.locations(vendor).then(locs => {
      setLocations(locs);
      const keys = Object.keys(locs);
      if (keys.length > 0 && !keys.includes(location)) setLocation(keys[0]);
    }).catch(err => toast.error('Locations load failed', { description: String(err) }));
  // location is intentionally NOT a dep — we just want it as the seed.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [vendor]);

  const currentLocation = locations[location];
  const availableLocations = useMemo(
    () => vendors.find(v => v.vendor === vendor)?.locations ?? [],
    [vendors, vendor],
  );

  // ---- Send a single packet (or burst from Manual tab) -------------------
  const sendOnce = useCallback(async (opts: { burst?: number; interval?: number } = {}) => {
    setSending(true);
    try {
      const r = await api.send({
        vendor,
        location,
        source_ip:   sourceIp || undefined,
        refcode:     refcode  || undefined,
        severity:    severityOverride || undefined,
        text:        textOverride || undefined,
        count:       opts.burst ?? 1,
        interval:    opts.interval ?? 0,
        use_gum:     useGum,
        rfc5424,
        random_pick: randomPick,
      });
      if (r.ok) {
        const first = r.records[0];
        toast.success(`Sent ${r.sent} trap${r.sent === 1 ? '' : 's'}`, {
          description: first
            ? `${first.severity} · ${first.array_name} → ${first.target}`
            : `${vendor} ${location}`,
        });
      } else {
        toast.error('Send failed', {
          description: r.errors.slice(0, 2).join(' • ') || 'unknown error',
        });
      }
    } catch (err) {
      toast.error('Network error', { description: String(err) });
    } finally {
      setSending(false);
    }
  }, [vendor, location, sourceIp, refcode, severityOverride, textOverride, useGum, rfc5424, randomPick]);

  return (
    <div className="app">
      <Toaster theme="dark" position="bottom-right" richColors closeButton />

      <header className="header">
        <div className="header__brand">
          <ShieldAlert size={20} className="header__brand-icon" />
          <div>
            <h1>UnifiedOps Trap Sender</h1>
            <p>dev tool · pushes syslog traps into the listener pipeline</p>
          </div>
        </div>
        <div className="header__status">
          {backendOk === null ? (
            <span className="pill pill--idle"><Loader2 size={12} className="spin" /> connecting…</span>
          ) : backendOk ? (
            <span className="pill pill--ok"><Wifi size={12} /> backend connected</span>
          ) : (
            <span className="pill pill--err"><WifiOff size={12} /> backend unreachable</span>
          )}
        </div>
      </header>

      <nav className="tabs">
        <button
          type="button"
          className={`tab ${tab === 'manual' ? 'tab--active' : ''}`}
          onClick={() => setTab('manual')}
        >
          <Send size={14} />
          Manual
        </button>
        <button
          type="button"
          className={`tab ${tab === 'schedule' ? 'tab--active' : ''}`}
          onClick={() => setTab('schedule')}
        >
          <Sparkles size={14} />
          Schedule
        </button>
        <button
          type="button"
          className={`tab ${tab === 'sannav' ? 'tab--active' : ''}`}
          onClick={() => { setTab('sannav'); setVendor('SANnav'); }}
        >
          <ShieldAlert size={14} />
          SANnav Events
        </button>
      </nav>

      <section className="picker">
        {tab !== 'sannav' && (
          <Field label="Vendor">
            <select value={vendor} onChange={(e) => setVendor(e.target.value)}>
              {vendors.filter(v => v.vendor !== 'SANnav').map(v => <option key={v.vendor} value={v.vendor}>{v.vendor}</option>)}
            </select>
          </Field>
        )}
        <Field label="Location">
          <select value={location} onChange={(e) => setLocation(e.target.value)}>
            {availableLocations.map(l => <option key={l} value={l}>{l}</option>)}
          </select>
        </Field>
        <Field label={`Source IP (${currentLocation?.ips.length ?? 0} known)`}>
          <select value={sourceIp} onChange={(e) => setSourceIp(e.target.value)}>
            <option value="">(random from pool)</option>
            {(currentLocation?.ips ?? []).map(x => (
              <option key={x.ip} value={x.ip}>{x.array} · {x.ip}</option>
            ))}
          </select>
        </Field>
        <Field label="Ref code">
          <select
            value={refcode}
            onChange={(e) => setRefcode(e.target.value)}
            disabled={randomPick}
          >
            <option value="">(catalogue default)</option>
            {catalog.map(c => (
              <option key={c.refcode} value={c.refcode}>
                {c.refcode} · {c.severity}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Severity override">
          <select value={severityOverride} onChange={(e) => setSeverityOverride(e.target.value)}>
            <option value="">(use catalogue)</option>
            <option value="Acute">Acute</option>
            <option value="Serious">Serious</option>
            <option value="Moderate">Moderate</option>
            <option value="Service">Service</option>
            <option value="Info">Info</option>
          </select>
        </Field>
        <Field label="Message override">
          <input
            type="text"
            value={textOverride}
            onChange={(e) => setTextOverride(e.target.value)}
            placeholder="(use catalogue text)"
          />
        </Field>
        {vendor === 'Hitachi' && (
          <>
            <Field label="Envelope">
              <div className="row gap-2">
                <Toggle on={useGum} onChange={setUseGum} labelOn="GUM" labelOff="SVP" />
              </div>
            </Field>
            <Field label="Format">
              <div className="row gap-2">
                <Toggle on={rfc5424} onChange={setRfc5424} labelOn="RFC5424" labelOff="RFC3164" />
              </div>
            </Field>
          </>
        )}
        <Field label="Random catalogue pick per send">
          <Toggle on={randomPick} onChange={setRandomPick} labelOn="On" labelOff="Off" />
        </Field>
      </section>

      <AnimatePresence mode="wait">
        {tab === 'manual' ? (
          <motion.section
            key="manual"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{    opacity: 0, y: -4 }}
            transition={{ duration: 0.18 }}
            className="panel"
          >
            <ManualPanel sending={sending} onSend={sendOnce} />
          </motion.section>
        ) : (
          <motion.section
            key="schedule"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{    opacity: 0, y: -4 }}
            transition={{ duration: 0.18 }}
            className="panel"
          >
            <SchedulePanel onSendNow={sendOnce} />
          </motion.section>
        ) : (
          <motion.section
            key="sannav"
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{    opacity: 0, y: -4 }}
            transition={{ duration: 0.18 }}
            className="panel"
          >
            <div className="panel__body" style={{ marginBottom: '1rem', color: 'var(--cyan-400)' }}>
              <p>Sending SANnav Alarms, Events, and Violations to the Health Check Report buckets.</p>
            </div>
            <ManualPanel sending={sending} onSend={sendOnce} />
          </motion.section>
        )}
      </AnimatePresence>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Manual panel
// ---------------------------------------------------------------------------
interface ManualPanelProps {
  sending: boolean;
  onSend:  (opts?: { burst?: number; interval?: number }) => Promise<void>;
}

function ManualPanel({ sending, onSend }: ManualPanelProps) {
  const [burst,    setBurst]    = useState(1);
  const [interval, setInterval] = useState(0.5);

  return (
    <div className="panel__body">
      <div className="row gap-3 wrap">
        <Field label="Packets per click">
          <input
            type="number"
            min={1}
            max={200}
            value={burst}
            onChange={(e) => setBurst(Math.max(1, Math.min(200, Number(e.target.value) || 1)))}
          />
        </Field>
        <Field label="Spacing (s)">
          <input
            type="number"
            min={0}
            step={0.1}
            max={10}
            value={interval}
            onChange={(e) => setInterval(Math.max(0, Math.min(10, Number(e.target.value) || 0)))}
          />
        </Field>
      </div>
      <div className="row gap-2 mt-2">
        <button
          type="button"
          className="btn btn--primary"
          disabled={sending}
          onClick={() => onSend({ burst, interval })}
        >
          {sending
            ? <><Loader2 size={14} className="spin" /> Sending…</>
            : <><Send size={14} /> Send {burst} trap{burst === 1 ? '' : 's'}</>
          }
        </button>
        <button
          type="button"
          className="btn"
          disabled={sending}
          onClick={() => onSend({ burst: 1, interval: 0 })}
        >
          <CheckCircle2 size={14} /> Send 1 now
        </button>
      </div>
      <p className="hint">
        <AlertTriangle size={12} />
        Traps go straight to the listener's UDP port for the selected
        location. No auto-fire — only what you click is sent.
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Schedule panel — client-side setInterval driving repeated /api/send calls.
// ---------------------------------------------------------------------------
interface SchedulePanelProps {
  onSendNow: (opts?: { burst?: number; interval?: number }) => Promise<void>;
}

function SchedulePanel({ onSendNow }: SchedulePanelProps) {
  const [intervalS, setIntervalS] = useState(10);
  const [burst,     setBurst]     = useState(1);
  const [running,   setRunning]   = useState(false);
  const [ticks,     setTicks]     = useState(0);
  const [startedAt, setStartedAt] = useState<number | null>(null);

  useEffect(() => {
    if (!running) return;
    setStartedAt(Date.now());
    setTicks(0);
    const id = window.setInterval(() => {
      void onSendNow({ burst });
      setTicks(t => t + 1);
    }, Math.max(1, intervalS) * 1000);
    // Fire one immediately so the operator sees something happen.
    void onSendNow({ burst });
    setTicks(1);
    return () => window.clearInterval(id);
  }, [running, intervalS, burst, onSendNow]);

  const uptime = running && startedAt
    ? Math.max(0, Math.floor((Date.now() - startedAt) / 1000))
    : 0;

  return (
    <div className="panel__body">
      <div className="row gap-3 wrap">
        <Field label="Every (seconds)">
          <input
            type="number"
            min={1}
            max={3600}
            value={intervalS}
            onChange={(e) => setIntervalS(Math.max(1, Math.min(3600, Number(e.target.value) || 1)))}
            disabled={running}
          />
        </Field>
        <Field label="Packets per tick">
          <input
            type="number"
            min={1}
            max={50}
            value={burst}
            onChange={(e) => setBurst(Math.max(1, Math.min(50, Number(e.target.value) || 1)))}
            disabled={running}
          />
        </Field>
      </div>
      <div className="row gap-2 mt-2">
        {!running ? (
          <button type="button" className="btn btn--primary" onClick={() => setRunning(true)}>
            <Play size={14} /> Start scheduler
          </button>
        ) : (
          <button type="button" className="btn btn--danger" onClick={() => setRunning(false)}>
            <Pause size={14} /> Stop scheduler
          </button>
        )}
      </div>
      {running && (
        <div className="schedule-status">
          <span className="pill pill--ok"><Loader2 size={12} className="spin" /> running</span>
          <span className="kv"><span className="kv__k">Ticks fired</span><span className="kv__v">{ticks}</span></span>
          <span className="kv"><span className="kv__k">Uptime</span><span className="kv__v">{uptime}s</span></span>
          <span className="kv"><span className="kv__k">Next in</span><span className="kv__v">≤ {intervalS}s</span></span>
        </div>
      )}
      <p className="hint">
        <AlertTriangle size={12} />
        Schedule runs entirely in this browser tab. Close the tab and
        the schedule stops. For a daemon-style scheduler use a cron job
        against the same backend.
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tiny UI helpers
// ---------------------------------------------------------------------------
function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="field">
      <span className="field__label">{label}</span>
      {children}
    </label>
  );
}

interface ToggleProps {
  on:        boolean;
  onChange:  (next: boolean) => void;
  labelOn:   string;
  labelOff:  string;
}

function Toggle({ on, onChange, labelOn, labelOff }: ToggleProps) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={on}
      className={`toggle ${on ? 'toggle--on' : ''}`}
      onClick={() => onChange(!on)}
    >
      <span className="toggle__pill" />
      <span className="toggle__label">{on ? labelOn : labelOff}</span>
    </button>
  );
}
