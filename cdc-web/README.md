# CDC EX management UI

Vue 3 and TypeScript, built with Vite. The production bundle contains its own code and styles; there are no CDN scripts, remote fonts, analytics, or runtime template compilation. English is the default language; the language control switches every interface label to Simplified Chinese.

## Build

Use Node.js 22.18+ (or a compatible newer LTS), then:

```bash
cd cdc-web
npm ci
npm run build
```

The exact dependency graph is recorded in `package-lock.json`. The output is `cdc-web/dist`; build it before packaging the server JAR. The server serves this directory's files from its packaged static resources.

For local development, run the management server on `127.0.0.1:8080`, then `npm run dev`. Vite proxies only `/api` to that local server. `npm run preview` serves the production bundle for layout review; it is not a replacement for the authenticated management server.

## Workflow

1. Sign in with the configured administrator bearer token. The token is stored only in this browser tab's `sessionStorage` and sent in an authorization header, never in a URL.
2. Add source and destination connections, then test their respective roles. Password and URI fields are masked; saved secrets are never loaded back into the editor. Leaving a protected field blank preserves the server-side value when editing.
3. Create a task through endpoint selection, explicit field mapping, and final review. Field discovery reads source and destination schemas. Keys cannot be excluded through the discovered mapping controls.
4. Validate and start the created task. The list shows capture state, pending events and acknowledged delivery. Pause/resume controls preserve checkpoints; retained errors remain visible.
5. Pause after draining the task and stop source writes before running a bounded comparison. The confirmation describes this operational requirement; the result does not pretend to be a coordinated live snapshot.

The DDL action shows a read-only proposal. Neither the UI nor its DDL endpoint executes schema changes. Source and destination connection tests can require different privileges and options. Connector configuration and the server's preflight are authoritative; unknown modes or unsupported mappings are not auto-corrected.

## Browser acceptance

The repository's `scripts/ui-smoke.mjs` drives the actual management API through a real browser. `scripts/security-smoke.sh` can prepare isolated MySQL source/target tables and run it with `CDC_TEST_UI=true`.

```bash
cd cdc-web
npm ci
npx playwright install --with-deps chromium
cd ..
CDC_TEST_SECURITY_DATABASE=true CDC_TEST_UI=true bash scripts/security-smoke.sh
```

The browser test requires `CDC_UI_BASE_URL`, `CDC_UI_TOKEN`, `CDC_UI_DB_PORT`, `CDC_UI_DB_USER`, and `CDC_UI_DB_PASSWORD`; the fixture supplies them. Optional `CDC_UI_SOURCE_DATABASE` / `CDC_UI_TARGET_DATABASE` default to `ui_src` / `ui_dst`. `CDC_UI_SECOND_TARGET_DATABASE` enables a second simultaneous task against the same source with a distinct replication client ID and empty destination. The security fixture supplies `ui_dst2` for this check. Each database must have the fixture's `items` table; the source contains one row and the destination is empty. These credentials and resources are for a disposable test environment, not an existing deployment.

The test creates connections through the forms, confirms redaction and credential preservation, discovers fields, creates and validates a task, starts real MySQL snapshots, checks acknowledged delivery, runs two tasks concurrently with separate replication identities, pauses and resumes, and verifies equal mapped data. It also checks English/Chinese navigation, mobile layout and browser errors. Desktop, mapping, comparison, concurrent-task and mobile screenshots are written to `CDC_UI_ARTIFACT_DIR` (or the `ui-artifacts` directory in the working directory). `CDC_UI_SCREENSHOT_DIR` remains accepted as a compatibility alias. `CDC_UI_CHROMIUM` can select an existing browser executable.

## Security and display behavior

The UI renders API content as escaped text; it does not use raw HTML interpolation. It works with a content security policy that allows scripts and styles only from the application's own origin and disallows inline execution. Requests have a timeout and actions remain disabled while pending. A pause or stop is displayed as in progress until the server reports its resulting state. Read-only metrics refresh every ten seconds while the tab is visible. Source-event age is labelled as the last event's age, not as a guarantee about replication lag.
