/**
 * Data layer for the RTT Command Centre OSDK app.
 *
 * One interface (DataApi), two implementations:
 *   - restApi  : default; talks to the Ontology REST API with a bearer token (works today).
 *   - osdkApi  : recommended for production; uncomment the generated-SDK import + TODO(OSDK) lines.
 *
 * Mirrors the adapter in app/rtt_command_centre.html, now type-safe.
 */

/* TODO(OSDK): after you Generate the SDK in Developer Console, import the objects/actions:
 *   import { RttPathway, rttTriagePathway } from '@rtt-programme/sdk';
 *   import client from './client';
 */

export interface Pathway {
  pathwayId: string;
  patientPseudoId?: string;
  trustCode: string;
  specialtyName: string;
  specialtyCode?: string;
  referralDate?: string;
  weeksWaited: number;
  pathwayStatus: string;
  waitBand?: string;
  ragStatus?: string;
  isBreach18w?: boolean;
  is52wBreach?: boolean;
  is65wBreach?: boolean;
  reviewStatus?: string;
  triageNote?: string;
}

export const TARGET = 18;
export const waitBand = (w: number): string =>
  w <= 18 ? '0-18' : w <= 26 ? '19-26' : w <= 39 ? '27-39' : w <= 51 ? '40-51' : w <= 64 ? '52-64' : '65+';
export const rag = (w: number): string => (w <= 18 ? 'GREEN' : w < 52 ? 'AMBER' : 'RED');
export const derive = (p: Pathway): Pathway => ({
  ...p,
  weeksWaited: Number(p.weeksWaited),
  waitBand: waitBand(Number(p.weeksWaited)),
  ragStatus: rag(Number(p.weeksWaited)),
  isBreach18w: Number(p.weeksWaited) > 18,
  is52wBreach: Number(p.weeksWaited) >= 52,
  is65wBreach: Number(p.weeksWaited) >= 65,
});

export interface DataApi {
  list(): Promise<Pathway[]>;
  triage(pathwayId: string, reviewStatus: string, triageNote: string): Promise<void>;
}

const env = (import.meta as any).env;
const HOST: string = env?.VITE_FOUNDRY_API_URL ?? '';
const ONTOLOGY: string = env?.VITE_FOUNDRY_ONTOLOGY_RID ?? '';
const TOKEN: string = env?.VITE_FOUNDRY_TOKEN ?? '';

/* ---------------------- REST implementation (default) ---------------------- */
export const restApi: DataApi = {
  async list() {
    const url = `${HOST.replace(/\/$/, '')}/api/v2/ontologies/${ONTOLOGY}/objects/RttPathway?pageSize=500`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
    if (!r.ok) throw new Error(`list ${r.status}`);
    const j = await r.json();
    return (j.data ?? []).map((o: any) => derive(o as Pathway));
  },
  async triage(pathwayId, reviewStatus, triageNote) {
    const url = `${HOST.replace(/\/$/, '')}/api/v2/ontologies/${ONTOLOGY}/actions/rtt-triage-pathway/apply`;
    const r = await fetch(url, {
      method: 'POST',
      headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ parameters: { pathway: pathwayId, reviewStatus, triageNote } }),
    });
    if (!r.ok) throw new Error(`triage ${r.status}`);
  },
};

/* ---------------------- Typed OSDK implementation (prod) ---------------------- */
export const osdkApi: DataApi = {
  async list() {
    // TODO(OSDK):
    // const page = await client(RttPathway).fetchPage({ $pageSize: 500 });
    // return page.data.map((o) => derive(o as unknown as Pathway));
    throw new Error('Wire the generated SDK in osdk.ts (see TODO(OSDK)).');
  },
  async triage(pathwayId, reviewStatus, triageNote) {
    // TODO(OSDK):
    // await client(rttTriagePathway).applyAction({ pathway: pathwayId, reviewStatus, triageNote });
    throw new Error('Wire the generated SDK in osdk.ts (see TODO(OSDK)).');
  },
};

/** Swap to `osdkApi` once the generated SDK is wired. */
export const dataApi: DataApi = restApi;
