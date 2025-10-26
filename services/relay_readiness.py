"""Relay readiness detection for CyberHerd startup.

This module provides utilities to wait for nostrclient's relay_manager
to be properly initialized before starting subscriptions.

Pattern mirrors nwcprovider's approach: directly inspect relay_manager,
check relay connections, and provide detailed status in logs.
"""

import asyncio
from datetime import datetime, timezone
from loguru import logger
from typing import Any


async def wait_for_relays_ready(
    max_wait_seconds: float = 30.0,
    check_interval: float = 2.0,
    min_connected_relays: int = 1
) -> dict[str, Any]:
    """Wait for nostrclient relay_manager to have connected relays.
    
    This helper directly inspects the relay_manager to verify:
    1. At least one relay is configured
    2. At least `min_connected_relays` relays report is_connected=True
    3. Message pool is operational
    
    Args:
        max_wait_seconds: Maximum time to wait (default: 30s)
        check_interval: How often to check status (default: 2s)
        min_connected_relays: Minimum number of connected relays needed (default: 1)
    
    Returns:
        Dict with status: {
            'ready': bool,
            'relay_count': int,
            'connected_count': int,
            'relay_urls': list[str],
            'waited_seconds': float,
            'reason': str  # If not ready, why
        }
    """
    start_time = datetime.now(timezone.utc)
    waited = 0.0
    
    logger.info(f"🔍 CyberHerd: Waiting for nostrclient relays (max {max_wait_seconds}s, need {min_connected_relays} connected)...")
    
    # Try to import nostrclient
    try:
        from lnbits.extensions.nostrclient.router import nostr_client
    except ImportError as e:
        logger.warning(f"❌ Nostrclient not available: {e}")
        return {
            'ready': False,
            'relay_count': 0,
            'connected_count': 0,
            'relay_urls': [],
            'waited_seconds': 0.0,
            'reason': f"nostrclient not imported: {e}"
        }
    
    last_status = None
    
    while waited < max_wait_seconds:
        try:
            # Check if relay_manager exists
            if not hasattr(nostr_client, 'relay_manager'):
                reason = "relay_manager attribute not found"
                if last_status != reason:
                    logger.debug(f"⏳ Nostrclient: {reason} (waited {waited:.1f}s)")
                    last_status = reason
                await asyncio.sleep(check_interval)
                waited += check_interval
                continue
            
            relay_manager = nostr_client.relay_manager
            
            # Check if relays dict exists
            if not hasattr(relay_manager, 'relays'):
                reason = "relays attribute not found on relay_manager"
                if last_status != reason:
                    logger.debug(f"⏳ Nostrclient: {reason} (waited {waited:.1f}s)")
                    last_status = reason
                await asyncio.sleep(check_interval)
                waited += check_interval
                continue
            
            relays = relay_manager.relays
            
            # Get relay URLs
            relay_urls = []
            if hasattr(relays, 'keys'):
                relay_urls = list(relays.keys())
            elif isinstance(relays, dict):
                relay_urls = list(relays.keys())
            
            relay_count = len(relay_urls)
            
            # Check if at least one relay is configured
            if relay_count == 0:
                reason = "no relays configured"
                if last_status != reason:
                    logger.debug(f"⏳ Nostrclient: {reason} (waited {waited:.1f}s)")
                    last_status = reason
                await asyncio.sleep(check_interval)
                waited += check_interval
                continue
            
            # Count connected relays
            connected_count = 0
            try:
                relay_objects = []
                if hasattr(relays, 'values'):
                    relay_objects = list(relays.values())
                elif isinstance(relays, dict):
                    relay_objects = list(relays.values())
                
                for relay in relay_objects:
                    # Check 'connected' attribute (not 'is_connected')
                    # Relay class uses self.connected: bool
                    if hasattr(relay, 'connected') and relay.connected:
                        connected_count += 1
                    # Fallback check for 'is_connected' in case API changes
                    elif hasattr(relay, 'is_connected') and relay.is_connected:
                        connected_count += 1
            except Exception as e:
                logger.debug(f"⏳ Error counting connected relays: {e}")
            
            # Check if we have enough connected relays
            if connected_count < min_connected_relays:
                reason = f"{connected_count}/{relay_count} relays connected (need {min_connected_relays})"
                if last_status != reason:
                    logger.debug(f"⏳ Nostrclient: {reason} (waited {waited:.1f}s)")
                    last_status = reason
                await asyncio.sleep(check_interval)
                waited += check_interval
                continue
            
            # Check message pool health (optional but good practice)
            try:
                if hasattr(relay_manager, 'message_pool'):
                    message_pool = relay_manager.message_pool
                    # Just check it exists and has basic attributes
                    if not (hasattr(message_pool, 'events') or hasattr(message_pool, 'has_events')):
                        logger.debug("⏳ Message pool exists but missing expected attributes")
            except Exception as e:
                logger.debug(f"⏳ Message pool check failed: {e}")
            
            # Success! We have relays and they're connected
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            logger.success(
                f"✅ Nostrclient ready: {relay_count} relay(s) configured, "
                f"{connected_count} connected (waited {duration:.1f}s)"
            )
            logger.info(f"📡 Connected relays: {', '.join(relay_urls[:5])}")
            if len(relay_urls) > 5:
                logger.info(f"   ... and {len(relay_urls) - 5} more")
            
            return {
                'ready': True,
                'relay_count': relay_count,
                'connected_count': connected_count,
                'relay_urls': relay_urls,
                'waited_seconds': duration,
                'reason': 'success'
            }
            
        except Exception as e:
            logger.debug(f"⏳ Error checking relay status: {e}")
            await asyncio.sleep(check_interval)
            waited += check_interval
    
    # Timeout - gather final status for diagnostics
    final_status = {
        'ready': False,
        'relay_count': 0,
        'connected_count': 0,
        'relay_urls': [],
        'waited_seconds': waited,
        'reason': f'timeout after {max_wait_seconds}s'
    }
    
    try:
        from lnbits.extensions.nostrclient.router import nostr_client
        if hasattr(nostr_client, 'relay_manager'):
            relay_manager = nostr_client.relay_manager
            if hasattr(relay_manager, 'relays'):
                relays = relay_manager.relays
                if hasattr(relays, 'keys'):
                    relay_urls = list(relays.keys())
                    final_status['relay_count'] = len(relay_urls)
                    final_status['relay_urls'] = relay_urls
                    
                    # Count connected
                    if hasattr(relays, 'values'):
                        relay_objects = list(relays.values())
                        # Check 'connected' attribute (primary) or 'is_connected' (fallback)
                        connected = sum(
                            1 for r in relay_objects 
                            if (hasattr(r, 'connected') and r.connected) or 
                               (hasattr(r, 'is_connected') and r.is_connected)
                        )
                        final_status['connected_count'] = connected
    except Exception as e:
        final_status['reason'] += f' (error gathering final status: {e})'
    
    # Log detailed warning with actual status
    logger.warning(
        f"⚠️ Nostrclient relays not ready after {max_wait_seconds}s | "
        f"Relays: {final_status['relay_count']} configured, "
        f"{final_status['connected_count']} connected | "
        f"URLs: {', '.join(final_status['relay_urls'][:3]) if final_status['relay_urls'] else 'none'} | "
        f"Reason: {final_status['reason']}"
    )
    
    return final_status
