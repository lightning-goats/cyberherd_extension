# CyberHerd functional defect repair

This change addresses all 23 confirmed defects and the two payout limitations from the 2026-10-24 audit. The finding-to-regression inventory is `2026-10-24-defect-closure.csv` in this directory.

## Owner-confirmed rules

Each eligible engagement member receives one percentage point of the entire feeder payment. Kinds 6 and 7 together still grant one bonus. Every new or rejoining zapper must meet the configured minimum; an active member may add any positive amount.

## Behavior changes

- F01–F08, F24: enforce settled incoming invoice authority, configured wallet/tracking scope, signed zap requests and authentic project notes. Payment recovery paginates. Historical refresh uses authenticated settled local invoices, with the same payment-hash identity as incremental accounting; relay receipts alone never grant totals.
- F02, F10: persist membership, victim changes, processed identity and payment accounting together. Rollback preserves retryability after interruption or storage failure. Notifications and target updates run after commit and admission-lock release.
- F04, F09, F13, F21: authenticate daily-note metadata and identity, revalidate returning members, validate Lightning addresses, and apply the minimum to new/rejoining zappers even when displacing a free member.
- F11–F12, F19, F22: validate manual mutations before changes, expose the literal prune route correctly, calculate API fallback shares with the actual allocation rules, and reject engagement budgets below configured herd capacity.
- F14–F17: track local membership reset days durably, include social-only opted-in users, handle named-zone DST and startup catchup, and preserve publishing-owner context in fallback notifications.
- F18, F23, F25: include allocation settings in target signatures, retain six decimal places of percentage precision, and keep stored payouts consistent with payable recipients. Serialize target snapshots/writes and resets so stale work cannot overwrite newer state.
- F20: independent balance notifications do not authorize automatic payouts. The feeder integration must report an accepted trigger, synchronize the triggering zap, and perform an uncached, owner-scoped target refresh before transfer. Any retryable intake/target storage failure blocks distribution.

## Paired deployment

Deploy CyberHerd **0.2.7** with Lightning Goats **0.2.2** from `lightning-goats/lightning_goats_extension`. The companion update supplies explicit feeder authorization; older companion versions delegate blindly and cannot drive the new automatic-payout path.

CyberHerd migrations37–38 add reset-day state and widen PostgreSQL payout storage to DOUBLE PRECISION. The initial reset-day migration records today's local day without altering current membership. Pre-upgrade reset history cannot be inferred; subsequent day changes are reconciled before admission and distribution. Restart the service after copying the reviewed sources, so migrations and new code load together. Preserve the database and extension backups for rollback; do not downgrade the widened payout column.

OpenHAB HTTP success establishes acceptance of its rule request. It does not establish physical food delivery. This repair does not create a physical feeder acknowledgement protocol or change OpenHAB rules. Daily-note posting also remains the responsibility of the project's external publishing process.

## Verification

Run in the existing LNbits virtualenv:

```
CYBERHERD_TEST_POSTGRES=1 python scripts/run_tests.py --env-file /path/to/lnbits/.env -q
```

The runner isolates extension imports, copies the suite outside the extension package root and blocks unscoped database access. PostgreSQL fixtures create and remove unique disposable schemas; SQLite uses temporary databases. Tests mock network publication, feeder operations and payment effects. The `--env-file` option is explicit; credentials are never printed.

Verification includes both SQL backends, real Schnorr-signed fixtures, failure injection, cancellation/restart identity, historical refresh ordering, raw SQL transaction rollback, reset/admission concurrency, split-write ordering, DST, fallback publishing toggles, API validation and the named explanation rules. No real test payments or feeder actions are required.
