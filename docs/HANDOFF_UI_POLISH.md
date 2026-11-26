# Post-M9 management UI polish

This is a presentation refinement on the validated 0.1.0-rc.1 base, not a CDC engine or data format change.

- Preserve authenticated API, task lifecycle, source/sink preflight, field mapping and data comparison contracts.
- Use readable typography, a restrained teal accent, a neutral gray workspace, distinct dark sidebar, unified controls and visible focus states.
- Show connector identity next to task endpoints, display configured host and port together, and expose full truncated names via titles.
- Present bounded comparison results as an explicit consistent/different/incomplete verdict with source and target counts. Detailed JSON remains available on demand. Counts alone or incomplete comparison do not establish consistency.
- English default; Simplified Chinese selectable. Phones and small tablets render task rows as complete cards with endpoints, state, counters and actions. Wide mapping tables retain their own scroll container.

## Verified on 2026-11-26

- TypeScript check and Vite production build passed.
- Java 21 Maven package (tests skipped for the packaging step) passed.
- Real Chromium browser acceptance passed against an isolated MySQL 8.4.13 fixture and the actual packaged server. Authentication, connection creation, preflight, field wizard, snapshot, pause/resume, concurrent tasks, comparison summary and expandable JSON, EN/ZH and mobile layout passed. The test asserts that the mobile task table and row actions fit the viewport. No browser errors.
- API security acceptance passed: authentication, redaction, encrypted persistence, ownership lock, key verification, remote-bind policy and key-file restart.
- The full repository CI is separate from this targeted verification. The previous RC release remains unchanged; no new release is implied.

## Actual browser screenshots

All images use disposable test data and contain no authentication secrets. They are captures from the working application, not mockups.

[Overview](screenshots/ui-polish/overview.png) · [Tasks](screenshots/ui-polish/tasks.png) · [Connections](screenshots/ui-polish/connections.png) · [Mapping](screenshots/ui-polish/mapping.png) · [Comparison](screenshots/ui-polish/comparison.png) · [Concurrent tasks](screenshots/ui-polish/concurrent-tasks.png) · [Login](screenshots/ui-polish/login.png) · [Chinese mobile](screenshots/ui-polish/mobile-zh.png)
