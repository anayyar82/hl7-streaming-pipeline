# HL7App — AppKit (TypeScript + Node.js)

This is the **Databricks AppKit** front end: **React 19**, **Vite**, **Express** (via `server/server.ts`), **TypeScript 5.9+**, and **`@databricks/appkit` / `appkit-ui`**.

The **Streamlit** app in `../hl7-forecasting-app/` remains the feature-complete UI until you port pages here.

## Prereqs

- **Node.js 22+** and `npm` 10+
- **Databricks CLI 0.295+** (for `databricks apps init` and deploys)

## Commands

```bash
cd hl7-appkit-app
npm install
npm run build        # dist/ + client/dist/ — required before Databricks run
npm run dev          # local dev (optional .env from .env.example)
```

## Deploy (Databricks)

1. `app.yaml` in this folder sets **Lakebase** and **Genie**-related env vars (aligned with the Streamlit app). Adjust for your workspace if needed.
2. **The AppKit app is a separate Databricks bundle** (the main `hl7_streaming_pipeline` bundle is limited to **one** git-sourced app: two apps in the same bundle error with *Duplicate app source code path*). After you deploy the pipeline with the main bundle, deploy this app from **`bundles/hl7_appkit/`**:

   ```bash
   cd bundles/hl7_appkit
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```

   The Databricks app name is **`hl7app-appkit`** (resource key in YAML is `hl7app_appkit`); git settings use the `app_git_*` variables in `resources/hl7_appkit_app_resource.yml`.
3. If the **Streamlit** app and this app share a service principal for Lakebase, you set `PGUSER` to the same `service_principal_client_id` (or update Lakebase role after the AppKit app is first created). Both apps’ `app.yaml` files are aligned, but the active SP UUID comes from the deployed app in your workspace.
4. **Build output must be in Git** or produced by your CI before the app runs `npm start`, unless you use a custom image or build step. The default `command` is `npm run start` (see `app.yaml`).

**Note:** For many teams, a CI job runs `npm ci && npm run build` and commits `dist/`, *or* you use a Dockerfile — confirm with your Databricks App deploy pipeline.

## Plugins

- Edit `server/server.ts` to add `lakebase()`, `genie()`, `analytics()`, etc. Run `appkit setup` and `appkit generate-types` after plugin changes (see [AppKit docs](https://databricks.github.io/appkit/docs/)).
- `appkit.plugins.json` lists available plugin manifests for this template.

## Source

Scaffolded with `databricks apps init --name hl7-appkit --output-dir hl7-appkit-app`. Reference demo: [dbxWearables](https://github.com/mkgs-databricks-demos/dbxWearables) (AppKit + DLT + Lakebase patterns).
