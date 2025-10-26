#!/usr/bin/env python3
"""Migration Guide for nostr_helpers.py

This script shows how to migrate existing code to use the new nostr_helpers module.
"""

# ============================================================================
# BEFORE (Direct nostrclient access)
# ============================================================================

# Old pattern - accessing nostrclient directly
from lnbits.extensions.nostrclient.router import nostr_client

# Check availability
if nostr_client and nostr_client.relay_manager:
    # Direct access to relay_manager
    nostr_client.relay_manager.add_subscription(sub_id, filters)
    
    # Direct access to message_pool
    while nostr_client.relay_manager.message_pool.has_events():
        event = nostr_client.relay_manager.message_pool.get_event()
        # process event...

# Old query pattern
async def old_query():
    from . import nostr_lookup as nl
    events = await nl._query_events_via_manager(filters, limit=50)
    return events


# ============================================================================
# AFTER (Using nostr_helpers)
# ============================================================================

# New pattern - use nostr_helpers
from . import nostr_helpers

# Check availability
if nostr_helpers.check_availability():
    # Add subscription via helper
    nostr_helpers.add_subscription(sub_id, filters)
    
    # Use message pool poller
    poller = nostr_helpers.create_message_pool_poller()
    while poller.has_events():
        event = poller.get_event()
        # process event...

# New query pattern
async def new_query():
    events = await nostr_helpers.query_events(filters, limit=50)
    return events


# ============================================================================
# MIGRATION EXAMPLES
# ============================================================================

# Example 1: Subscription Management
# ----------------------------------
# BEFORE:
def old_add_subscription():
    from lnbits.extensions.nostrclient.router import nostr_client
    nostr_client.relay_manager.add_subscription("my_sub", [{"kinds": [1]}])

# AFTER:
def new_add_subscription():
    from . import nostr_helpers
    nostr_helpers.add_subscription("my_sub", [{"kinds": [1]}])


# Example 2: Message Pool Polling
# -------------------------------
# BEFORE:
async def old_poll():
    from lnbits.extensions.nostrclient.router import nostr_client
    pool = nostr_client.relay_manager.message_pool
    
    while pool.has_events():
        event_msg = pool.get_event()
        if event_msg.subscription_id == "my_sub":
            # process
            pass
        else:
            pool.events.put(event_msg)  # put back

# AFTER:
async def new_poll():
    from . import nostr_helpers
    poller = nostr_helpers.create_message_pool_poller()
    
    while poller.has_events():
        event_msg = poller.get_event()
        if event_msg.subscription_id == "my_sub":
            # process
            pass
        else:
            poller.put_event_back(event_msg)  # put back


# Example 3: Event Queries
# ------------------------
# BEFORE:
async def old_query_notes():
    from . import nostr_lookup as nl
    filters = {"kinds": [1], "authors": [pubkey], "limit": 10}
    events = await nl._query_events_via_manager(filters, limit=10, timeout=5.0)
    return events

# AFTER:
async def new_query_notes():
    from . import nostr_helpers
    filters = {"kinds": [1], "authors": [pubkey], "limit": 10}
    events = await nostr_helpers.query_events(filters, limit=10, timeout=5.0)
    return events


# Example 4: Relay Information
# ----------------------------
# BEFORE:
def old_get_relay_info():
    from lnbits.extensions.nostrclient.router import nostr_client
    relays = nostr_client.relay_manager.relays
    return {"relay_count": len(relays)}

# AFTER:
def new_get_relay_info():
    from . import nostr_helpers
    return nostr_helpers.get_relay_info()
    # Returns: {'available': True, 'relay_count': 5, 'connected_count': 4, ...}


# Example 5: Subscription Tracking
# --------------------------------
# BEFORE (manual tracking):
class OldMonitor:
    def __init__(self):
        self.subscription_ids = []
    
    def add_sub(self, sub_id, filters):
        from lnbits.extensions.nostrclient.router import nostr_client
        nostr_client.relay_manager.add_subscription(sub_id, filters)
        self.subscription_ids.append(sub_id)
    
    def close_all(self):
        from lnbits.extensions.nostrclient.router import nostr_client
        for sub_id in self.subscription_ids:
            nostr_client.relay_manager.close_subscription(sub_id)
        self.subscription_ids.clear()

# AFTER (using SubscriptionManager):
class NewMonitor:
    def __init__(self):
        from . import nostr_helpers
        self.sub_manager = nostr_helpers.create_subscription_manager()
    
    def add_sub(self, sub_id, filters):
        self.sub_manager.add(sub_id, filters)
    
    def close_all(self):
        self.sub_manager.close_all()


# ============================================================================
# FILES THAT NEED MIGRATION
# ============================================================================

"""
Files currently using direct nostrclient access:

1. services/nostr_event_monitor.py
   - Line 225: nostr_client.relay_manager.add_subscription()
   - Line 276: nostr_client.relay_manager.add_subscription()
   - Line 298-320: Direct message_pool access
   - Line 339: pool.events.put()

2. services/nostr_lookup.py
   - Line 24-165: _query_events_via_manager() implementation
   - Can be refactored to use nostr_helpers.query_events()

3. services/headbutt.py
   - Line 112: nl._query_events_via_manager()
   - Should use nostr_helpers.query_events()

4. views_api.py
   - Several places import nostr_lookup
   - Can be updated to use nostr_helpers

5. crud.py
   - Line 1610: imports nostr_lookup
   - Can use nostr_helpers

6. services/zap_monitor.py
   - Line 29: imports nostr_lookup
   - Can use nostr_helpers
"""


# ============================================================================
# MIGRATION CHECKLIST
# ============================================================================

"""
Phase 1: Update nostr_event_monitor.py
[ ] Replace add_subscription calls
[ ] Replace message_pool polling
[ ] Test subscriptions still work

Phase 2: Update nostr_lookup.py
[ ] Deprecate _query_events_via_manager
[ ] Add wrapper that calls nostr_helpers.query_events()
[ ] Keep backward compatibility for now

Phase 3: Update other services
[ ] headbutt.py: Use nostr_helpers.query_events()
[ ] zap_monitor.py: Import nostr_helpers
[ ] views_api.py: Use nostr_helpers
[ ] crud.py: Use nostr_helpers

Phase 4: Testing
[ ] Test event queries
[ ] Test subscriptions
[ ] Test message pool polling
[ ] Test relay reconnections
[ ] Test error handling

Phase 5: Cleanup
[ ] Remove old nostr_lookup patterns
[ ] Update all imports
[ ] Update documentation
"""


if __name__ == "__main__":
    print(__doc__)
    print("\nThis is a migration guide. See code comments for examples.")
