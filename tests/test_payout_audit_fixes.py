"""Regressions for allocation changes, payable recipients and fractional shares."""
from types import SimpleNamespace
import pytest
from lnbits.extensions.cyberherd.services import shares, splits, send_splits

@pytest.fixture
def anyio_backend():return 'asyncio'

def member(pubkey,amount,kinds='9735',address=None):
 return dict(pubkey=pubkey,amount=amount,kinds=kinds,is_active=1,
             lud16=address if address is not None else pubkey+'@example.test',display_name=pubkey)

def test_remaining_pool_preserves_fractional_proportions():
 rows=[member('a',0,'6,7'),member('b',300),member('c',700)]
 assert shares.compute_member_share_percentages(rows,10)=={'a':0.039841,'b':2.988048,'c':6.972111}

@pytest.mark.anyio
async def test_config_change_updates_targets_and_alias(monkeypatch):
 config=SimpleNamespace(zap_wallet='goat-wallet',zap_wallet_alias='Goats',member_allocation_percent=10)
 async def settings(*a,**k):return config
 async def members(*a,**k):return [member('a',100)]
 writes=[]
 async def set_targets(source,targets):writes.append([(t.wallet,t.percent,t.alias) for t in targets])
 monkeypatch.setattr(splits.crud,'get_settings',settings)
 monkeypatch.setattr(splits.crud,'get_active_cyberherd_members',members)
 monkeypatch.setattr(splits,'split_set_targets',set_targets)
 monkeypatch.setattr(splits,'_split_signature_cache',{})
 await splits.update_split_targets_proportional('source',user_id='owner')
 config.member_allocation_percent=20
 config.zap_wallet_alias='Updated Goats'
 await splits.update_split_targets_proportional('source',user_id='owner')
 assert writes==[[('goat-wallet',90,'Goats'),('a@example.test',10,'a')],
                 [('goat-wallet',80,'Updated Goats'),('a@example.test',20,'a')]]

@pytest.mark.anyio
async def test_invalid_addresses_are_excluded_and_fractional_targets_preserved(monkeypatch):
 config=SimpleNamespace(zap_wallet='goat-wallet',zap_wallet_alias='Goats',member_allocation_percent=10)
 async def settings(*a,**k):return config
 async def members(*a,**k):return [member('a',0,'6'),member('b',300),member('c',700),member('d',10000,address='not-an-address')]
 async def set_targets(source,targets):pass
 monkeypatch.setattr(splits.crud,'get_settings',settings)
 monkeypatch.setattr(splits.crud,'get_active_cyberherd_members',members)
 monkeypatch.setattr(splits,'split_set_targets',set_targets)
 monkeypatch.setattr(splits,'_split_signature_cache',{})
 result=await splits.update_split_targets_proportional('source',user_id='owner')
 assert {t['wallet']:t['percent'] for t in result['targets']}==pytest.approx({
  'goat-wallet':90,'a@example.test':0.02991,'b@example.test':2.991027,'c@example.test':6.979063}, abs=1e-6)

@pytest.mark.anyio
async def test_balance_threshold_alone_never_authorizes_payout(monkeypatch):
 config=SimpleNamespace(send_splits_enabled=True,feeder_trigger_sats=1000,herd_wallet='herd',source_wallet='source')
 async def resolve(wallet):return wallet,SimpleNamespace(balance_msat=1000000)
 transferred=[]
 async def transfer(settings):transferred.append(True);return {'ok':True}
 monkeypatch.setattr(send_splits,'_resolve_wallet',resolve)
 monkeypatch.setattr(send_splits,'transfer_herd_balance_to_source',transfer)
 monkeypatch.setattr(send_splits,'_LAST_ATTEMPT',{})
 result=await send_splits.maybe_send_splits_for_user('owner',config)
 assert transferred==[]
 assert result is None

@pytest.mark.anyio
@pytest.mark.parametrize('threshold,balance',[(1000,1000000),(1000,900000),(None,1000000)])
async def test_successful_feeder_context_can_distribute(monkeypatch,threshold,balance):
 config=SimpleNamespace(user_id='owner',send_splits_enabled=True,feeder_trigger_sats=threshold,herd_wallet='herd',source_wallet='source')
 async def resolve(wallet):return wallet,SimpleNamespace(balance_msat=balance)
 async def transfer(settings, **kwargs):return {'ok':True,'sats':1000}
 monkeypatch.setattr(send_splits,'_resolve_wallet',resolve)
 monkeypatch.setattr(send_splits,'transfer_herd_balance_to_source',transfer)
 monkeypatch.setattr(send_splits,'_LAST_ATTEMPT',{})
 assert await send_splits.maybe_send_splits_for_user('owner',config,feeder_triggered=True)=={'ok':True,'sats':1000}

@pytest.mark.anyio
async def test_reset_target_failure_propagates(monkeypatch):
 async def settings(*a,**k):return SimpleNamespace(user_id='owner',zap_wallet=None)
 async def fail(*a,**k):raise RuntimeError('target store unavailable')
 monkeypatch.setattr(splits.crud,'get_settings',settings)
 monkeypatch.setattr(splits,'split_set_targets',fail)
 with pytest.raises(RuntimeError,match='target store unavailable'):
  await splits.reset_splits_to_predefined_wallet('source',user_id='owner')

@pytest.mark.anyio
async def test_reset_cannot_be_overwritten_by_an_inflight_old_target_write(monkeypatch):
 import asyncio
 config=SimpleNamespace(user_id='owner',zap_wallet='goat-wallet',zap_wallet_alias='Goats',member_allocation_percent=10)
 async def settings(*a,**k):return config
 async def members(*a,**k):return [member('a',100)]
 started=asyncio.Event();release=asyncio.Event();stored=[]
 async def set_targets(source,targets):
  if len(targets)>1:
   started.set();await release.wait()
  stored[:]=[(t.wallet,t.percent) for t in targets]
 monkeypatch.setattr(splits.crud,'get_settings',settings)
 monkeypatch.setattr(splits.crud,'get_active_cyberherd_members',members)
 monkeypatch.setattr(splits,'split_set_targets',set_targets)
 monkeypatch.setattr(splits,'_split_signature_cache',{})
 monkeypatch.setattr(splits,'_target_locks',{},raising=False)
 old=asyncio.create_task(splits.update_split_targets_proportional('source',user_id='owner'))
 await started.wait()
 reset=asyncio.create_task(splits.reset_splits_to_predefined_wallet('source',user_id='owner'))
 await asyncio.sleep(0)
 release.set()
 await asyncio.gather(old,reset)
 assert stored==[('goat-wallet',100)]

@pytest.mark.anyio
async def test_strict_pretransfer_refresh_never_uses_cached_target_state(monkeypatch):
 config=SimpleNamespace(user_id='owner',source_wallet='source',zap_wallet='goats',zap_wallet_alias='Goats',member_allocation_percent=10)
 async def settings(*a,**k):return config
 async def members(*a,**k):return [member('a',100)]
 persisted=[]
 async def store(source,targets):persisted[:]=[(t.wallet,t.percent) for t in targets]
 monkeypatch.setattr(splits.crud,'get_settings',settings)
 monkeypatch.setattr(splits.crud,'get_active_cyberherd_members',members)
 monkeypatch.setattr(splits,'split_set_targets',store)
 monkeypatch.setattr(splits,'_split_signature_cache',{})
 await splits.update_split_targets_proportional('source',user_id='owner')
 persisted[:]=[('manually-changed',100)]
 await splits.update_split_targets_proportional('source',user_id='owner',strict=True)
 assert persisted==[('goats',90),('a@example.test',10)]

@pytest.mark.anyio
@pytest.mark.parametrize('failure',['settings','owner','source','clear'])
async def test_strict_refresh_rejects_unresolved_settings_and_failed_target_clear(monkeypatch,failure):
 config=SimpleNamespace(user_id='other' if failure=='owner' else 'owner',source_wallet='other' if failure=='source' else 'source',zap_wallet=None)
 async def settings(*a,**k):
  if failure=='settings':raise RuntimeError('settings unavailable')
  return config
 async def members(*a,**k):return []
 async def store(*a,**k):raise RuntimeError('target clear failed')
 monkeypatch.setattr(splits.crud,'get_settings',settings)
 monkeypatch.setattr(splits.crud,'get_active_cyberherd_members',members)
 monkeypatch.setattr(splits,'split_set_targets',store)
 monkeypatch.setattr(splits,'_split_signature_cache',{})
 with pytest.raises(RuntimeError):
  await splits.update_split_targets_proportional('source',user_id='owner',strict=True)
