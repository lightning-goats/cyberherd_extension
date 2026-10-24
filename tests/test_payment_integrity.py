"""Settled invoice identity, admission recovery and authenticated history regressions."""
import asyncio
import hashlib
import json
import os
import time
from types import SimpleNamespace
from uuid import uuid4

import coincurve
import pytest
from sqlalchemy.sql import text
import lnbits.db as db_module
from lnbits.db import Database, POSTGRES, SQLITE
from lnbits.settings import settings as core_settings
from lnbits.extensions.cyberherd import crud, migrations
from lnbits.extensions.cyberherd.models import CyberherdSettings
from lnbits.extensions.cyberherd.services import payment_coordinator as pc, headbutt, messaging, nostr_helpers, zap_totals
from lnbits.core.models.payments import Payment

OWNER = 'payment-integrity-owner'
PROJECT = coincurve.PrivateKey(bytes.fromhex('11' * 32))
ALICE = coincurve.PrivateKey(bytes.fromhex('22' * 32))
OTHER = coincurve.PrivateKey(bytes.fromhex('33' * 32))

def pk(key):
    return key.public_key_xonly.format().hex()

def event(key, kind, tags, created_at=None, content=''):
    ev = dict(pubkey=pk(key), created_at=created_at or int(time.time()), kind=kind, tags=tags, content=content)
    raw = json.dumps([0, ev['pubkey'], ev['created_at'], kind, tags, content], separators=(',', ':'), ensure_ascii=False).encode()
    ev['id'] = hashlib.sha256(raw).hexdigest()
    ev['sig'] = key.sign_schnorr(bytes.fromhex(ev['id'])).hex()
    assert nostr_helpers.verify_event_signature(ev)
    return ev

def payment(zap, payment_hash='1' * 64, status='success'):
    return Payment(checking_id=payment_hash, payment_hash=payment_hash, wallet_id='integrity-herd', amount=20000, fee=0, bolt11='test-invoice', status=status, extra={'nostr': json.dumps(zap)})

@pytest.fixture
def anyio_backend():
    return 'asyncio'

@pytest.fixture(params=[SQLITE, POSTGRES])
async def scenario(request, tmp_path, monkeypatch):
    backend = request.param
    url = core_settings.lnbits_database_url
    if backend == POSTGRES and not (os.getenv('CYBERHERD_TEST_POSTGRES') and url):
        pytest.skip('set CYBERHERD_TEST_POSTGRES=1 for disposable PostgreSQL schemas')
    monkeypatch.setattr(db_module, 'DB_TYPE', backend)
    monkeypatch.setattr(core_settings, 'lnbits_database_url', url if backend == POSTGRES else None)
    monkeypatch.setattr(core_settings, 'lnbits_data_folder', str(tmp_path))
    db = Database('ext_payment_integrity_' + uuid4().hex)
    monkeypatch.setattr(crud, 'db', db)
    monkeypatch.setattr(headbutt, '_HEADBUTT_LOCK', None)
    note = event(PROJECT, 1, [['t', 'CyberHerd']])
    s = CyberherdSettings(user_id=OWNER, herd_wallet='integrity-herd', zap_tracking_enabled=True, nostr_pubkey_override=pk(PROJECT), tracked_tags=['#CyberHerd'], tracked_event_ids=[note['id']], tracked_event_timestamps={note['id']: int(time.time())}, nip05_verification_enabled=False)
    data = s.dict()
    schema = db.references_schema
    fields = [name + ' ' + ('BIGINT' if isinstance(value, (int, bool)) else 'TEXT') for name, value in data.items()]
    await db.execute(f'CREATE TABLE {schema}settings (' + ','.join(fields) + ')')
    params = {key: json.dumps(value) if isinstance(value, (list, dict)) else int(value) if isinstance(value, bool) else value for key, value in data.items()}
    await db.execute(f'INSERT INTO {schema}settings (' + ','.join(params) + ') VALUES (' + ','.join(':' + key for key in params) + ')', params)
    await crud._bootstrap_cyberherd_tables()
    await db.execute(f"ALTER TABLE {schema}cyber_herd ADD COLUMN allocation_percentage REAL DEFAULT 0")
    await db.execute(f"CREATE TABLE {schema}membership_reset_days (user_id TEXT PRIMARY KEY, local_day TEXT NOT NULL, splits_pending INTEGER NOT NULL DEFAULT 0)")
    await db.execute(f"INSERT INTO {schema}cyber_herd(pubkey,user_id,display_name,lud16,nprofile,amount,is_active,kinds) VALUES (:pk,:owner,'Alice','alice@example.test','nprofile-test',100,1,'9735')", {'pk': pk(ALICE), 'owner': OWNER})
    events = {note['id']: note}
    async def external_queries(filters, *args, **kwargs):
        if 'ids' in filters:
            return [events[nid] for nid in filters['ids'] if nid in events]
        return [ev for ev in events.values() if ev['kind'] in filters.get('kinds', []) and ev['pubkey'] in filters.get('authors', [])]
    async def external_publish(*args, **kwargs):
        return True
    async def external_get_wallet(*args, **kwargs):
        return None
    monkeypatch.setattr(nostr_helpers, 'query_events', external_queries)
    monkeypatch.setattr(messaging, 'publish_event_message', external_publish)
    monkeypatch.setattr(messaging, 'send_cyberherd_update', external_publish)
    import lnbits.core.crud as core_crud
    monkeypatch.setattr(core_crud, 'get_wallet', external_get_wallet)
    try:
        yield SimpleNamespace(db=db, settings=s, note=note, events=events, service=pc.PaymentCoordinatorService(user_id=OWNER, app=SimpleNamespace()))
    finally:
        if backend == POSTGRES:
            await db.execute(f'DROP SCHEMA IF EXISTS {db.schema} CASCADE')
        await db.engine.dispose()

async def amount():
    return (await crud.get_cyberherd_member_by_pubkey(pk(ALICE), OWNER))['amount']

def request_for(scenario, kind=9734, recipient=None):
    return event(ALICE, kind, [['e', scenario.note['id']], ['p', recipient or pk(PROJECT)]])

@pytest.mark.anyio
@pytest.mark.parametrize('status', ['pending', 'failed'])
async def test_only_settled_invoices_credit_members(scenario, status):
    pay = payment(request_for(scenario), status=status)
    assert await scenario.service._process_payment_for_zap(pay) is False
    assert await amount() == 100
    assert not await crud.is_event_processed(OWNER, pay.payment_hash)
    pay.status = 'success'
    assert await scenario.service._process_payment_for_zap(pay)
    assert await amount() == 120

@pytest.mark.anyio
@pytest.mark.parametrize('field,value', [('zap_tracking_enabled', 0), ('herd_wallet', None), ('herd_wallet', 'other-wallet')])
async def test_intake_requires_enabled_scoped_herd_wallet(scenario, field, value):
    await scenario.db.execute(f'UPDATE {scenario.db.references_schema}settings SET {field}=:value WHERE user_id=:owner', {'value': value, 'owner': OWNER})
    assert await scenario.service._process_payment_for_zap(payment(request_for(scenario))) is False
    assert await amount() == 100

@pytest.mark.anyio
async def test_signed_non_zap_event_is_rejected(scenario):
    assert await scenario.service._process_payment_for_zap(payment(request_for(scenario, kind=1))) is False
    assert await amount() == 100

@pytest.mark.anyio
async def test_recipient_claim_does_not_override_actual_note_author(scenario):
    foreign = event(OTHER, 1, [], content='foreign note')
    scenario.events[foreign['id']] = foreign
    zap = event(ALICE, 9734, [['e', foreign['id']], ['p', pk(PROJECT)]])
    assert await scenario.service._process_payment_for_zap(payment(zap)) is False
    assert await amount() == 100

@pytest.mark.anyio
async def test_lock_cancellation_does_not_consume_invoice(scenario):
    pay = payment(request_for(scenario))
    lock = headbutt._get_headbutt_lock()
    await lock.acquire()
    task = asyncio.create_task(scenario.service._process_payment_for_zap(pay))
    try:
        await asyncio.sleep(.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        lock.release()
    assert not await crud.is_event_processed(OWNER, pay.payment_hash)
    assert await pc.PaymentCoordinatorService(user_id=OWNER, app=SimpleNamespace())._process_payment_for_zap(pay)
    assert await amount() == 120

@pytest.mark.anyio
async def test_distinct_settled_hashes_share_request_but_both_count_once(scenario):
    zap = request_for(scenario)
    for h in ['1' * 64, '2' * 64, '1' * 64]:
        assert await scenario.service._process_payment_for_zap(payment(zap, payment_hash=h))
    assert await amount() == 140
    row = await crud.get_zap_totals_row(OWNER, pk(ALICE))
    assert row['total_sats'] == 40 and row['event_count'] == 2

@pytest.mark.anyio
async def test_recovery_paginates_past_processed_prefix(scenario, monkeypatch):
    import lnbits.core.services.payments as core_payments
    zap = request_for(scenario)
    history = [payment(zap, payment_hash=f'{n:064x}') for n in range(1, 1002)]
    schema = scenario.db.references_schema
    async with scenario.db.engine.begin() as conn:
        await conn.execute(text(f'INSERT INTO {schema}processed_events(user_id,event_hash,note_id,processed_at,event_type) VALUES (:owner,:hash,:note,1,\'payment_zap\')'), [dict(owner=OWNER, hash=p.payment_hash, note=scenario.note['id']) for p in history[:1000]])
    calls = []
    async def external_payments(**kwargs):
        calls.append(kwargs)
        filters = kwargs.get('filters')
        start = filters.offset if filters else kwargs.get('offset', 0)
        limit = filters.limit if filters else kwargs.get('limit', 1000)
        return history[start:start + limit]
    monkeypatch.setattr(core_payments, 'get_payments', external_payments)
    result = await scenario.service._recover_missed_payment_zaps(scenario.settings)
    assert result['scanned'] == 1001 and result['successful'] == 1
    assert all(call.get('complete') is True for call in calls)
    assert await amount() == 120

@pytest.mark.anyio
@pytest.mark.parametrize('invalid', ['pending', 'signature', 'recipient', 'wallet'])
async def test_historical_rebuild_rejects_unauthenticated_or_unsettled_contributions(scenario, monkeypatch, invalid):
    zap = request_for(scenario, recipient=pk(OTHER) if invalid == 'recipient' else None)
    if invalid == 'signature':
        zap['sig'] = '00' * 64
    pay = payment(zap, status='pending' if invalid == 'pending' else 'success')
    if invalid == 'wallet':
        pay.wallet_id = 'wrong-wallet'
    calls = []
    async def external_payments(**kwargs):
        calls.append(kwargs)
        return [pay] if len(calls) == 1 else []
    monkeypatch.setattr(zap_totals, 'get_payments', external_payments)
    result = await zap_totals.rebuild_zap_totals_from_payments(user_id=OWNER)
    assert result['zap_candidates'] == 0
    assert await crud.get_zap_totals_row(OWNER, pk(ALICE)) is None

@pytest.mark.anyio
async def test_unit_of_work_rolls_back_member_and_marker_on_cancel(scenario):
    with pytest.raises(asyncio.CancelledError):
        async with crud.admission_transaction():
            await crud.update_and_activate_member(pk(ALICE), 20, user_id=OWNER)
            assert await crud.register_processed_event(OWNER, '9' * 64, note_id=scenario.note['id'])
            raise asyncio.CancelledError()
    assert await amount() == 100
    assert not await crud.is_event_processed(OWNER, '9' * 64)

@pytest.mark.anyio
@pytest.mark.parametrize('invalid', [None, 'sender', 'signature', 'description', 'recipient'])
async def test_relay_receipt_without_settled_local_invoice_never_credits(scenario, monkeypatch, invalid):
    from bolt11 import Bolt11, MilliSatoshi, Tags, Tag, TagChar, encode
    request = request_for(scenario)
    if invalid == 'sender':
        request = event(OTHER, 9734, request['tags'])
    if invalid == 'signature':
        request['sig'] = '00' * 64
    description = json.dumps(request)
    invoice = Bolt11(currency='bc', date=int(time.time()), amount_msat=MilliSatoshi(20000), tags=Tags([Tag(TagChar.payment_hash, 'a' * 64), Tag(TagChar.payment_secret, 'b' * 64), Tag(TagChar.description_hash, hashlib.sha256(description.encode()).hexdigest())]))
    encoded = encode(invoice, private_key='33' * 32)
    receipt = event(OTHER, 9735, [['p', pk(OTHER) if invalid == 'recipient' else pk(PROJECT)], ['e', scenario.note['id']], ['bolt11', encoded], ['description', description + ' ' if invalid == 'description' else description]])
    async def external_relay(filters, *args, **kwargs):
        if filters.get('authors') and receipt['pubkey'] not in filters['authors']:
            return []
        return [receipt]
    monkeypatch.setattr(zap_totals, 'query_events', external_relay, raising=False)
    async def no_local_payments(**kwargs):
        return []
    monkeypatch.setattr(zap_totals, 'get_payments', no_local_payments)
    result = await zap_totals.get_zap_totals_for_zapper(user_id=OWNER, zapper_pubkey=pk(ALICE), force_refresh=True)
    assert result['total_sats'] == 0

@pytest.mark.anyio
async def test_total_failure_rolls_back_credit_and_claim_for_retry(scenario, monkeypatch):
    original = crud.upsert_zap_totals_row
    async def unavailable_total(**kwargs):
        raise RuntimeError('disposable simulated total write failure')
    monkeypatch.setattr(crud, 'upsert_zap_totals_row', unavailable_total)
    pay = payment(request_for(scenario))
    assert await scenario.service._process_payment_for_zap(pay) is False
    assert await amount() == 100
    assert not await crud.is_event_processed(OWNER, pay.payment_hash)
    assert await crud.get_processed_zap(OWNER, pay.payment_hash) is None
    monkeypatch.setattr(crud, 'upsert_zap_totals_row', original)
    assert await scenario.service._process_payment_for_zap(pay)
    assert await amount() == 120
    assert (await crud.get_zap_totals_row(OWNER, pk(ALICE)))['total_sats'] == 20

@pytest.mark.anyio
async def test_busy_history_lock_does_not_hold_database_transaction(scenario):
    lock = await zap_totals._get_key_lock((OWNER, pk(ALICE)))
    await lock.acquire()
    task = asyncio.create_task(scenario.service._process_payment_for_zap(payment(request_for(scenario))))
    try:
        await asyncio.sleep(.1)
        assert await asyncio.wait_for(amount(), timeout=.5) == 100
    finally:
        lock.release()
        await task

@pytest.mark.anyio
async def test_after_commit_effects_never_run_on_rollback(scenario):
    effects = []
    async def effect():
        effects.append(await amount())
    with pytest.raises(RuntimeError, match='rollback'):
        async with crud.admission_transaction():
            crud.defer_after_commit(effect)
            await crud.update_and_activate_member(pk(ALICE), 20, user_id=OWNER)
            raise RuntimeError('rollback')
    assert effects == []
    async with crud.admission_transaction():
        crud.defer_after_commit(effect)
        await crud.update_and_activate_member(pk(ALICE), 20, user_id=OWNER)
    assert effects == [120]

@pytest.mark.anyio
async def test_below_minimum_settled_payment_records_history_without_admission(scenario):
    await crud.deactivate_cyberherd_member(pk(ALICE), OWNER)
    await scenario.db.execute(f'UPDATE {scenario.db.references_schema}settings SET minimum_sats=50 WHERE user_id=:owner', {'owner': OWNER})
    pay = payment(request_for(scenario))
    assert await scenario.service._process_payment_for_zap(pay)
    member = await crud.get_cyberherd_member_by_pubkey(pk(ALICE), OWNER)
    assert not member['is_active'] and member['amount'] == 0
    assert await crud.is_event_processed(OWNER, pay.payment_hash)
    row = await crud.get_zap_totals_row(OWNER, pk(ALICE))
    assert row['total_sats'] == 20 and row['event_count'] == 1
    assert await pc.PaymentCoordinatorService(user_id=OWNER, app=SimpleNamespace())._process_payment_for_zap(pay)
    assert (await crud.get_zap_totals_row(OWNER, pk(ALICE)))['total_sats'] == 20

@pytest.mark.anyio
async def test_missing_required_metadata_does_not_consume_payment(scenario, monkeypatch):
    await crud.deactivate_cyberherd_member(pk(ALICE), OWNER)
    await scenario.db.execute(f'UPDATE {scenario.db.references_schema}settings SET nip05_verification_enabled=1 WHERE user_id=:owner', {'owner': OWNER})
    async def missing_metadata(self, attacker):
        return {}, 'unavailable'
    monkeypatch.setattr(headbutt.EnhancedHeadbuttService, '_resolve_attacker_metadata', missing_metadata)
    pay = payment(request_for(scenario))
    assert await scenario.service._process_payment_for_zap(pay) is False
    assert not await crud.is_event_processed(OWNER, pay.payment_hash)
    assert await amount() == 0
    assert await crud.get_zap_totals_row(OWNER, pk(ALICE)) is None
    await scenario.db.execute(f'UPDATE {scenario.db.references_schema}settings SET nip05_verification_enabled=0 WHERE user_id=:owner', {'owner': OWNER})
    assert await pc.PaymentCoordinatorService(user_id=OWNER, app=SimpleNamespace())._process_payment_for_zap(pay)
    assert await amount() == 20

@pytest.mark.anyio
async def test_valid_history_rebuild_counts_distinct_settled_invoice_hashes(scenario, monkeypatch):
    zap = request_for(scenario)
    history = [payment(zap, payment_hash='1' * 64), payment(zap, payment_hash='2' * 64)]
    calls = []
    async def external_payments(**kwargs):
        calls.append(kwargs)
        return history if len(calls) == 1 else []
    monkeypatch.setattr(zap_totals, 'get_payments', external_payments)
    result = await zap_totals.rebuild_zap_totals_from_payments(user_id=OWNER)
    assert result['zap_candidates'] == 2
    row = await crud.get_zap_totals_row(OWNER, pk(ALICE))
    assert row['total_sats'] == 40 and row['event_count'] == 2
    assert await amount() == 100


@pytest.mark.anyio
@pytest.mark.parametrize('refresh_first', [False, True])
async def test_authoritative_refresh_and_incremental_invoice_share_identity(scenario, monkeypatch, refresh_first):
    zap = request_for(scenario)
    pay = payment(zap)
    from bolt11 import Bolt11, MilliSatoshi, Tags, Tag, TagChar, encode
    description = json.dumps(zap)
    invoice = Bolt11(currency='bc', date=int(time.time()), amount_msat=MilliSatoshi(20000), tags=Tags([Tag(TagChar.payment_hash, pay.payment_hash), Tag(TagChar.payment_secret, 'b' * 64), Tag(TagChar.description_hash, hashlib.sha256(description.encode()).hexdigest())]))
    pay.bolt11 = encode(invoice, private_key='33' * 32)
    receipt = event(OTHER, 9735, [['p', pk(PROJECT)], ['e', scenario.note['id']], ['bolt11', pay.bolt11], ['description', description]])
    async def provider_receipts(*args, **kwargs):
        return [receipt]
    monkeypatch.setattr(zap_totals, 'query_events', provider_receipts, raising=False)
    async def local_payments(**kwargs):
        filters = kwargs.get('filters')
        return [pay] if not filters or not filters.offset else []
    monkeypatch.setattr(zap_totals, 'get_payments', local_payments)
    async def refresh():
        result = await zap_totals.get_zap_totals_for_zapper(user_id=OWNER, zapper_pubkey=pk(ALICE), force_refresh=True)
        assert result['total_sats'] == 20 and result['event_count'] == 1
    if refresh_first:
        await refresh()
    assert await scenario.service._process_payment_for_zap(pay)
    await refresh()
    await refresh()
    assert await amount() == 120
    row = await crud.get_zap_totals_row(OWNER, pk(ALICE))
    assert row['total_sats'] == 20 and row['event_count'] == 1
    assert json.loads(row['last_event_ids']) == [pay.payment_hash]

@pytest.mark.anyio
@pytest.mark.parametrize('case', ['ordinary', 'non_zap', 'foreign_note', 'missing_note', 'database_failure'])
async def test_distribution_sync_blocks_only_retryable_intake(scenario, monkeypatch, case):
    pay = payment(request_for(scenario, kind=1 if case == 'non_zap' else 9734))
    if case == 'ordinary':
        pay.extra = {}
    if case == 'foreign_note':
        foreign = event(OTHER, 1, [], content='foreign')
        scenario.events[foreign['id']] = foreign
        pay = payment(event(ALICE, 9734, [['e', foreign['id']], ['p', pk(PROJECT)]]))
    if case == 'missing_note':
        scenario.events.clear()
    if case == 'database_failure':
        async def fail(**kwargs):
            raise RuntimeError('disposable failure')
        monkeypatch.setattr(crud, 'upsert_zap_totals_row', fail)
    if case in ('missing_note', 'database_failure'):
        with pytest.raises(RuntimeError, match='retry'):
            await scenario.service.sync_payment_for_distribution(pay, scenario.settings)
    else:
        await scenario.service.sync_payment_for_distribution(pay, scenario.settings)

@pytest.mark.anyio
async def test_persisted_payouts_preserve_micro_percent_shares(scenario):
    if scenario.db.type == POSTGRES:
        # Represent an existing installation's REAL=float32 column, then run
        # the actual upgrade before recomputing from authoritative scores.
        await scenario.db.execute(f"ALTER TABLE {scenario.db.references_schema}cyber_herd ALTER COLUMN payouts TYPE REAL")
    await migrations.m038_preserve_payout_precision(scenario.db)
    for key in (OTHER, PROJECT):
        await scenario.db.execute(f"INSERT INTO {scenario.db.references_schema}cyber_herd(pubkey,user_id,display_name,lud16,nprofile,amount,is_active,kinds) VALUES (:pk,:owner,'Member','member@example.test','nprofile-test',100,1,'9735')", {'pk': pk(key), 'owner': OWNER})
    await crud.recompute_member_payouts(OWNER)
    rows = await crud.get_active_cyberherd_members(OWNER)
    assert sorted(row['payouts'] for row in rows) == pytest.approx([.33333333, .33333333, .33333334], abs=1e-10)
    assert sum(row['payouts'] for row in rows) == pytest.approx(1, abs=1e-10)

@pytest.mark.anyio
@pytest.mark.parametrize('tamper', ['created_at', 'pubkey', 'id'])
async def test_forged_project_note_metadata_cannot_credit_invoice(scenario, tamper):
    note = scenario.events[scenario.note['id']]
    if tamper == 'created_at':
        note['created_at'] += 1
    elif tamper == 'pubkey':
        note['pubkey'] = pk(OTHER)
    else:
        note['id'] = 'f' * 64
    pay = payment(request_for(scenario))
    assert await scenario.service._process_payment_for_zap(pay) is False
    assert await amount() == 100
    assert not await crud.is_event_processed(OWNER, pay.payment_hash)

@pytest.mark.anyio
async def test_committed_invoice_sync_survives_later_relay_outage(scenario):
    pay = payment(request_for(scenario))
    assert await scenario.service._process_payment_for_zap(pay)
    scenario.events.clear()
    assert await scenario.service.sync_payment_for_distribution(pay, scenario.settings)
    assert await amount() == 120
    assert (await crud.get_zap_totals_row(OWNER, pk(ALICE)))['total_sats'] == 20

@pytest.mark.anyio
async def test_persisted_payouts_exclude_addressless_and_inactive_scores(scenario):
    schema = scenario.db.references_schema
    await scenario.db.execute(f'UPDATE {schema}settings SET zap_wallet=:wallet, member_allocation_percent=10 WHERE user_id=:owner', {'wallet': 'predefined-zap-wallet', 'owner': OWNER})
    for key, address, active in [(OTHER, None, 1), (PROJECT, 'inactive@example.test', 0)]:
        await scenario.db.execute(f"INSERT INTO {schema}cyber_herd(pubkey,user_id,display_name,lud16,nprofile,amount,is_active,kinds) VALUES (:pk,:owner,'Member',:address,'nprofile-test',100,:active,'9735')", {'pk': pk(key), 'owner': OWNER, 'address': address, 'active': active})
    await crud.recompute_member_payouts(OWNER)
    rows = {row['pubkey']: row for row in await crud.get_all_cyberherd_members(OWNER)}
    assert rows[pk(ALICE)]['payouts'] == pytest.approx(.10)
    assert rows[pk(OTHER)]['payouts'] == 0
    assert rows[pk(PROJECT)]['payouts'] == 0
