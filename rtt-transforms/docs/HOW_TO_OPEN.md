# How to open the HTML deliverables

Two self-contained HTML files ship in this repository. Neither needs a build step —
just download and double-click. (They pull React / Mermaid from public CDNs, so the
first open needs internet.)

| File | What it is |
|---|---|
| `docs/pipeline_explainer.html` | Detailed, illustrated theory: architecture, step-by-step code flow, data-quality decision tree, incremental patterns, ontology ER, lineage, the 50-problem map and the test strategy. **Read this first.** |
| `app/rtt_command_centre.html` | An interactive **React** app — the RTT Command Centre / PTL — with full **CRUD + Triage** over the Pathway/Trust ontology, seeded with the real data. |

## Open them
1. In Foundry, open this Code Repository → **Files** tab.
2. Navigate to `docs/pipeline_explainer.html` (or `app/rtt_command_centre.html`).
3. Use **⋯ → Download** to save the file locally.
4. **Double-click** the downloaded file — it opens in your browser (Chrome/Edge/Firefox).
5. Keep both files in the same folder as your existing `Foundry_Learning_Journey.html`
   for one complete package.

> Tip: to view straight from the repo without downloading, use a browser extension that
> renders raw repo HTML, or serve the folder locally: `python -m http.server` then browse
> to `http://localhost:8000/`.

## Using the React Command Centre
- **Read:** KPI cards (Active PTL size, % within 18 weeks vs the 92% standard, 52/65-week
  breach counts) + a sortable, filterable PTL table (RAG traffic-lights, breach badges).
- **Create:** **+ New pathway** — pick trust/specialty/date/weeks; band, RAG and breach flags
  are derived automatically (same rules as `lib/rtt_metrics.py`).
- **Update:** **Edit** any pathway, or **Triage** it (records `review_status` + `triage_note`,
  mirroring the *Triage Pathway* Ontology action).
- **Delete:** **Del** removes a pathway (in production a soft-delete action preserving audit).

### Going live on the Ontology (OSDK)
The app reads/writes through a `DataSource` adapter. The default `InMemoryDataSource`
works offline; to run against real Foundry data, replace it with the documented
`OsdkDataSource` (see the comment block at the top of the `<script>` in
`app/rtt_command_centre.html`): generate the Ontology SDK from Developer Console, then
map `list()` to `client(RttPathway)` and `triage()` to the `rttTriagePathway` action.
Nothing else in the UI changes.

## How this relates to `Foundry_Learning_Journey.html`
- `Foundry_Learning_Journey.html` — **teaches the platform** (concepts, datasets, code repos,
  ontology, Workshop, OSDK, AIP, git): the *course*.
- `docs/pipeline_explainer.html` — **documents this pipeline** and how it solves all 50
  problems: the *reference*.
- `app/rtt_command_centre.html` — **demonstrates the operational app** the pipeline powers:
  the *product*.

## Related docs
- `docs/TRACEABILITY.md` — the full 50-problem matrix + dataset RID index + test strategy.
- `docs/SPECS.md` — Automation / Security / Visibility / Workshop / AIP specs.
