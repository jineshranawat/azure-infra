import React, { useEffect, useMemo, useState } from 'react';
import { dataApi, Pathway, TARGET } from './osdk';

const REVIEW = ['Validated', 'Needs escalation', 'Data query', 'Removed in error'];

/**
 * Core Command Centre wired to the live Ontology via osdk.ts.
 * Port the remaining tabs (Trusts, Analytics, Ontology, Data Health, Audit, Connect)
 * from app/rtt_command_centre.html - the logic is identical, just swap the in-memory
 * store for `dataApi`.
 */
export default function App() {
  const [rows, setRows] = useState<Pathway[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [f, setF] = useState({ trust: '', rag: '', status: 'Active' });
  const [q, setQ] = useState('');
  const [sortK, setSortK] = useState<keyof Pathway>('weeksWaited');
  const [sortDir, setSortDir] = useState(-1);
  const [triage, setTriage] = useState<Pathway | null>(null);

  const load = () => {
    setLoading(true);
    dataApi.list().then((r) => { setRows(r); setErr(null); })
      .catch((e) => setErr(e.message)).finally(() => setLoading(false));
  };
  useEffect(load, []);

  const trusts = useMemo(() => Array.from(new Set(rows.map((r) => r.trustCode))).sort(), [rows]);
  const filtered = useMemo(() => rows.filter((r) =>
    (!f.trust || r.trustCode === f.trust) &&
    (!f.rag || r.ragStatus === f.rag) &&
    (!f.status || r.pathwayStatus === f.status) &&
    (!q || `${r.pathwayId} ${r.specialtyName} ${r.trustCode}`.toLowerCase().includes(q.toLowerCase()))
  ), [rows, f, q]);
  const sorted = useMemo(() => [...filtered].sort((a, b) => {
    const x = a[sortK] as any, y = b[sortK] as any; return (x > y ? 1 : x < y ? -1 : 0) * sortDir;
  }), [filtered, sortK, sortDir]);

  const active = rows.filter((r) => r.pathwayStatus === 'Active');
  const within = active.filter((r) => r.weeksWaited <= TARGET).length;
  const pct = active.length ? Math.round((1000 * within) / active.length) / 10 : 0;

  const doTriage = async (id: string, s: string, n: string) => {
    try { await dataApi.triage(id, s, n); setTriage(null); load(); }
    catch (e: any) { alert(e.message); }
  };
  const th = (k: keyof Pathway, l: string) => (
    <th onClick={() => { setSortK(k); setSortDir((d) => (sortK === k ? -d : 1)); }} style={{ cursor: 'pointer' }}>
      {l}{sortK === k ? (sortDir > 0 ? ' \u25B2' : ' \u25BC') : ''}
    </th>
  );

  return (
    <div>
      <header>
        <div className="logo">RTT</div>
        <div><h1>RTT Command Centre</h1><div className="sub">OSDK &middot; live Ontology</div></div>
        <div className="spacer" /><button onClick={load}>Refresh</button>
      </header>
      <main>
        {err && <div className="err">Data error: {err}. Check .env (host / ontology / token) and CORS.</div>}
        <div className="kpis">
          <div className="kpi"><div className="l">Active PTL</div><div className="v">{active.length}</div></div>
          <div className="kpi"><div className="l">% within 18w</div><div className="v" style={{ color: pct >= 92 ? '#12b886' : '#f59f00' }}>{pct}%</div></div>
          <div className="kpi"><div className="l">52-week breaches</div><div className="v">{active.filter((r) => r.is52wBreach).length}</div></div>
          <div className="kpi"><div className="l">65-week breaches</div><div className="v">{active.filter((r) => r.is65wBreach).length}</div></div>
        </div>
        <div className="bar">
          <select value={f.status} onChange={(e) => setF({ ...f, status: e.target.value })}><option value="">All statuses</option><option>Active</option><option>Discharged</option></select>
          <select value={f.trust} onChange={(e) => setF({ ...f, trust: e.target.value })}><option value="">All trusts</option>{trusts.map((t) => <option key={t}>{t}</option>)}</select>
          <select value={f.rag} onChange={(e) => setF({ ...f, rag: e.target.value })}><option value="">All RAG</option><option>GREEN</option><option>AMBER</option><option>RED</option></select>
          <input placeholder="Search id / specialty / trust..." value={q} onChange={(e) => setQ(e.target.value)} />
          <span className="spacer" /><span className="muted">{sorted.length} rows</span>
        </div>
        {loading ? <p className="muted">Loading from the Ontology...</p> : (
          <table>
            <thead><tr>{th('pathwayId', 'Pathway')}{th('trustCode', 'Trust')}{th('specialtyName', 'Specialty')}{th('weeksWaited', 'Weeks')}{th('waitBand', 'Band')}{th('ragStatus', 'RAG')}<th>Breach</th>{th('pathwayStatus', 'Status')}<th>Review</th><th>Action</th></tr></thead>
            <tbody>
              {sorted.slice(0, 200).map((r) => (
                <tr key={r.pathwayId}>
                  <td><b>{r.pathwayId}</b></td><td>{r.trustCode}</td><td>{r.specialtyName}</td><td><b>{r.weeksWaited}</b></td><td>{r.waitBand}</td>
                  <td><span className={'chip ' + r.ragStatus}>{r.ragStatus}</span></td>
                  <td>{r.is65wBreach ? <span className="badge on">65w</span> : r.is52wBreach ? <span className="badge on">52w</span> : '-'}</td>
                  <td>{r.pathwayStatus}</td><td>{r.reviewStatus || '-'}</td>
                  <td><button onClick={() => setTriage(r)}>Triage</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </main>
      {triage && <TriageModal row={triage} onClose={() => setTriage(null)} onSave={doTriage} />}
    </div>
  );
}

function TriageModal({ row, onClose, onSave }: { row: Pathway; onClose: () => void; onSave: (id: string, s: string, n: string) => void }) {
  const [s, setS] = useState(row.reviewStatus || 'Validated');
  const [n, setN] = useState(row.triageNote || '');
  return (
    <div className="overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h3>Triage {row.pathwayId}</h3>
        <div className="muted">{row.trustCode} &middot; {row.specialtyName} &middot; {row.weeksWaited}w</div>
        <label>Review status</label>
        <select value={s} onChange={(e) => setS(e.target.value)}>{REVIEW.map((o) => <option key={o}>{o}</option>)}</select>
        <label>Note</label>
        <textarea value={n} onChange={(e) => setN(e.target.value)} />
        <div className="actions"><button onClick={onClose}>Cancel</button><button className="primary" onClick={() => onSave(row.pathwayId, s, n)}>Submit triage</button></div>
      </div>
    </div>
  );
}
