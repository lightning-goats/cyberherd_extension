# CyberHerd audit defect repair implementation plan

> **For agentic workers:** Use systematic debugging, test-driven development and independent final review. The user has authorized implementation, deployment, confirmation and push.

**Goal:** Correct the 23 confirmed defects and the two payout consistency/precision limitations from the functional audit.

**Architecture:** Keep the existing extension interfaces. Couple membership and idempotency markers in a database transaction; validate all ingress paths; persist daily reset state; route automatic payout through a successful feeder trigger boundary. Preserve existing wallet ownership and user scoping.

**Tech Stack:** Python, FastAPI, LNbits Database, SQLite/PostgreSQL, pytest.

**Spec:** `docs/audit/2026-10-24-defect-repair.md`, `docs/audit/2026-10-24-defect-closure.csv`, `cyberherd_explanation.md`, owner clarifications below.

## Global constraints

- Engagement grants 1% of the full feeder payment, once for kinds 6/7 combined.
- Every new/rejoining zap meets the minimum; active members can add any positive amount.
- Never send real test payments, publish test Nostr messages or activate a feeder for verification.
- Preserve unrelated LNbits worktree changes and current live membership on initial reset-state migration.
- No new dependencies; no credential output.

## Review focus

- Cancellation before/after payment and social marker persistence must cause neither lost effects nor duplicate credit.
- Provider-signed receipts, forged/pending invoices and unrelated note authors must be handled consistently.
- Midnight startup, disabled zap tracking, DST and repeated reset attempts must respect the correct owner's day.
- Malformed/missing recipient addresses and allocation changes must produce coherent payable targets and API fields.
- Blocked/failed/unknown feeder actions must never authorize automatic payout.

## Task 1: Payment intake, historical totals and atomic persistence

Owner: payment agent. Files: `crud.py`, `services/payment_coordinator.py`, `services/zap_totals.py`, new regression tests.

- [x] Convert F01/F02/F03/F05/F06/F07/F08/F24 reproductions into intended-behavior failures.
- [x] Add ContextVar-scoped transaction support shared with admission; enforce settled incoming payments and configured scope; verify request kind and referenced author; paginate recovery; validate history and query provider receipts correctly.
- [x] Align stored payouts with eligible recipient allocation and preserve fractional percentages (F25).
- [x] Run SQLite/PostgreSQL regressions and report findings coverage.

## Task 2: Admission, social validation and API contracts

Owner: admission agent. Files: `services/headbutt.py`, `services/subscriptions.py`, `views_api.py`, `utils/common.py`, new regression tests.

- [x] Write intended-behavior failures for F04/F09/F10/F11/F12/F13/F19/F21/F22.
- [x] Use shared transaction for marker/member mutations; validate current note and rejoining identity/address; enforce minimum on every newcomer; validate API writes before changes; register prune route correctly; repair shares fallback and incompatible settings validation.
- [x] Run genuine signed-event/real CRUD regressions with external effects mocked.

## Task 3: Lifecycle, time and notifications

Owner: lifecycle agent. Files: `tasks.py`, `services/time_utils.py`, `services/messaging.py`, `migrations.py`, `__init__.py`, new tests.

- [x] Write failures for F14/F15/F16/F17.
- [x] Persist local reset day with non-destructive initial migration; catch up before listeners; include social-only owners; use named system timezone; carry publishing owner through fallback path.
- [x] Verify restarts, DST and owner toggles with isolated tests.

## Task 4: Split targets and feeder integration

Owner: root. Files: `services/shares.py`, `services/splits.py`, `services/send_splits.py`, companion Lightning Goats integration if necessary, tests.

- [x] Write failures for F18/F20/F23 and target-side F13/F25.
- [x] Include allocation settings in target signature; use shared recipient validation; preserve fractional proportional shares; gate automatic transfers on feeder result and remove competing autonomous distribution.
- [x] Verify failure/override/no-ack behavior without physical actions.

## Task 5: Integration, deploy, confirm and publish

- [ ] Run the full extension suite and relevant companion tests with SQLite/PostgreSQL; review the combined patch independently.
- [ ] Resolve review findings, create release notes and finding-to-test closure tracker; commit intended files only.
- [ ] Back up installed extension files; deploy exact checked source, run migrations through normal startup and restart with authorized service controls.
- [ ] Confirm health, source/runtime equality, new schema/reset state, member state and split targets without real payments or feeder actions.
- [ ] Push reviewed commits to the appropriate remotes and verify remote HEADs.
