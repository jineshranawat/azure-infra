# Deployment runbook (UI steps)

Full illustrated version: **`docs/engine_command_centre.html`** (sections A–E). This is the
quick markdown copy. *Verify names against your current tenant.*

## A · Build & schedule the pipeline
1. Code Repositories → `rtt-transforms` → confirm latest commit **Checks** are green.
2. Open `rtt_ptl` → **Build** (builds all required inputs in order).
3. Automate: `rtt_ptl` → **··· → Schedules → New schedule** → trigger **Time 06:00**, scope
   **Build required inputs** → Save + Enable.
4. Event/SLA triggers: **Automate → New automation** (trigger on new raw transaction; notify on
   freshness). See `SPECS.md#automation`.

## B · Publish the ontology
1. Open the branch **Proposal (PR)** "RTT-Programme — The 50 Problems, Solved".
2. Review ontology (Trust, Pathway, link, Triage action) + code diff.
3. **Merge / Deploy to main**.
4. Ontology Manager → verify **Pathway** (15 props, link, Triage action) → **Sync** if needed.

## C · Apply PII markings
1. Control Panel → **Markings** → create/choose **"NHS Patient PII"**.
2. Add it to every dataset still carrying `nhs_number`: raw, typed, clean, minimised, metrics,
   rejects (**··· → Manage → Markings → Add**). It propagates downstream.
3. `rtt_patient_pseudonymised` is PII-free — no marking needed.

## D · Deploy the React app as an OSDK app
1. **Developer Console → + New application → Client-facing application.**
2. Select Ontology resources: `RttPathway`, `RttTrust`, action `rtt-triage-pathway`, the link.
3. **Generate SDK → Generate first version** (regenerate on any Ontology change).
4. Redirect URL `http://localhost:8080/auth/callback`; **Control Panel → CORS** allow
   `http://localhost:8080` **before** local dev.
5. **Create code repository** (bootstraps React + TypeScript + Vite + OAuth).
6. **Open in VS Code** / **Work locally**; set env `VITE_FOUNDRY_CLIENT_ID`,
   `VITE_FOUNDRY_REDIRECT_URL`, `VITE_FOUNDRY_API_URL`, `VITE_FOUNDRY_ONTOLOGY_RID`.
7. Drop in the **ready-made source** from `osdk-app/` (`src/App.tsx`, `src/osdk.ts`,
   `src/styles.css`, `src/client.ts`, `src/main.tsx`). It already talks to the Ontology; set the
   generated SDK import at `TODO(OSDK)` in `osdk-app/src/osdk.ts`:
   ```ts
   const page = await client(RttPathway).fetchPage({ $pageSize: 200 });
   await client(rttTriagePathway).applyAction({ pathway: id, reviewStatus, triageNote });
   ```
   (Reference full-featured UI: `app/rtt_command_centre.html`; see `osdk-app/README.md`.)
8. `npm install && npm run dev`; then `npm run lint && npm run test && npm run build`.
9. Release: `git tag 1.0.0 && git push origin tag 1.0.0` (or Code Repositories UI) → checks pass.
10. **Developer Console → Website hosting** → upload `dist/` (or `@osdk/cli`).
    URL `<subdomain>.<enrollment>.palantirfoundry.com`.
11. **Sharing → Share hosted website** (Viewer = website only).

**Gotchas:** CORS before local dev; regenerate SDK on Ontology change; default **CSP** blocks
non-Foundry URLs silently (check `F12`); redirect URL must match in `.env`, `index.html`,
Developer Console; hosting is **static only** (server logic → Functions).

**Fastest alternative:** keep the UI as a **Workshop** module (`SPECS.md#workshop`).

## E · Open the HTML files
1. Repo → **Files** → `docs/` or `app/` → select → **··· → Download** → double-click.
2. Or `python -m http.server` in the folder → `http://localhost:8000/`.
