# RTT Command Centre — OSDK React app (deploy-ready source)

Productionised, deployable version of `app/rtt_command_centre.html`, wired to the **live
Ontology** via the Ontology SDK. Source-only here (this repo's CI builds Python transforms,
not this Vite app) — you deploy it through Developer Console.

## Two ways to ship it

### Path 1 — Developer Console bootstrap (recommended)
1. Follow `docs/DEPLOYMENT.md` §D to create the Client-facing application, select `RttPathway`
   / `RttTrust` / `rtt-triage-pathway`, and **Generate the SDK**. Note the generated npm
   package name (e.g. `@rtt-programme/sdk`).
2. Developer Console → **Create code repository** (bootstraps React + TS + Vite + OAuth with the
   SDK dependency already wired).
3. Copy `src/App.tsx`, `src/osdk.ts`, `src/styles.css` from here into that repo. In `src/osdk.ts`
   set the generated SDK import (see `TODO(OSDK)`).
4. `npm install && npm run dev` → verify → `npm run build` → `git tag 1.0.0` → checks →
   Developer Console **Website hosting** → deploy `dist/` → **Share**.

### Path 2 — Standalone build
Use `package.json`, `vite.config.ts`, `tsconfig.json`, `index.html`, `.env.example` here:
`cp .env.example .env`, fill values, `npm install`, `npm run dev`. Requires CORS allow-listing
for your dev origin (Control Panel → CORS).

## Data layer (`src/osdk.ts`)
Two implementations behind one interface:
- **REST** (default; works immediately with host + ontology + token in `.env`).
- **Typed OSDK** (recommended for prod) — uncomment the generated-SDK import + the `TODO(OSDK)` lines.

## Env (`.env`)
```
VITE_FOUNDRY_API_URL=https://jinesh.euw-3.palantirfoundry.co.uk
VITE_FOUNDRY_ONTOLOGY_RID=ri.ontology.main.ontology.b31cb1df-52ef-4612-9a6d-fdf042f6f609
VITE_FOUNDRY_CLIENT_ID=<Developer Console OAuth client id>
VITE_FOUNDRY_REDIRECT_URL=http://localhost:8080/auth/callback
VITE_FOUNDRY_TOKEN=<dev-only bearer token for REST mode; omit in OAuth/prod>
```

## Ontology resources
| Resource | API name | RID |
|---|---|---|
| Pathway object | `RttPathway` | `ri.ontology.main.object-type.921abe22-2ab5-4ffd-b85c-9b6f17c68352` |
| Trust object | `RttTrust` | `ri.ontology.main.object-type.da8bc071-f125-4889-a403-df3b85168f9c` |
| Triage action | `rtt-triage-pathway` | `ri.actions.main.action-type.897cd6d1-e47d-420e-8e31-9aa3dd7d68a8` |

## Gotchas (OSDK docs)
CORS **before** local dev · **regenerate SDK** on ontology change · default **CSP** blocks
non-Foundry URLs silently (`F12`) · redirect URL must match in `.env`/`index.html`/Developer
Console · hosting is **static only** · Ontology **write** actions need the object types deployed
on `main` (merge the PR first).
