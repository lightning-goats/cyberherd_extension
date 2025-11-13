"""Task management for WebSocket-based Nostr monitors.

This module handles the lifecycle of per-user WebSocket monitors:
- Starting monitors for users with tracking enabled
- Stopping monitors when users disable tracking
- Periodic checks for new users

Follows the nwcprovider pattern for task management.
"""

import asyncio
from typing import Dict

from loguru import logger

from .nostr_websocket_monitor import (
    start_monitor_for_user,
    stop_monitor_for_user,
    get_active_monitors,
)
from .subscriptions import get_effective_pubkey
from ..crud import get_all_settings


async def handle_websocket_monitors(app):
    """Main task to manage WebSocket monitors for all users.
    
    This runs continuously and:
    1. Starts monitors for users with tracking enabled
    2. Stops monitors for users who disabled tracking
    3. Checks every 60 seconds for changes
    
    Follows the nwcprovider pattern (handle_nwc function).
    
    Args:
        app: FastAPI app instance (required for event processing)
    """
    logger.debug("🚀 Starting WebSocket monitor management task")
    
    try:
        while True:
            try:
                # Get all user settings
                all_settings = await get_all_settings()
                
                # Build set of user IDs that should have monitors
                users_needing_monitors = set()
                for settings in all_settings:
                    # User needs monitor if they have:
                    # 1. Tracked notes already (for engagement monitoring), OR
                    # 2. Tracking enabled (pubkey + tags configured for new notes)
                    has_tracked_notes = settings.tracked_event_ids and len(settings.tracked_event_ids) > 0
                    has_tracking_enabled = (
                        getattr(settings, 'tracked_tags', []) and 
                        get_effective_pubkey(settings)
                    )
                    
                    if has_tracked_notes or has_tracking_enabled:
                        users_needing_monitors.add(settings.user_id)
                
                # Get currently active monitors
                active_monitors = get_active_monitors()
                currently_active = set(active_monitors.keys())
                
                # Start monitors for users who need them but don't have them
                users_to_start = users_needing_monitors - currently_active
                for user_id in users_to_start:
                    try:
                        logger.debug(f"▶️  Starting monitor for user {user_id}")
                        await start_monitor_for_user(user_id, app)
                    except Exception as e:
                        logger.error(f"Failed to start monitor for user {user_id}: {e}")
                
                # Stop monitors for users who no longer need them
                users_to_stop = currently_active - users_needing_monitors
                for user_id in users_to_stop:
                    try:
                        logger.debug(f"⏹️  Stopping monitor for user {user_id} (no tracked notes)")
                        await stop_monitor_for_user(user_id)
                    except Exception as e:
                        logger.error(f"Failed to stop monitor for user {user_id}: {e}")
                
                # Ensure existing monitors are still running (restart if their tasks died)
                current_monitors = get_active_monitors()
                for user_id, monitor in current_monitors.items():
                    if monitor.is_running():
                        continue
                    try:
                        logger.warning(
                            f"♻️  WebSocket monitor for user {user_id} was not running; restarting"
                        )
                        await monitor.start()
                    except Exception as e:
                        logger.error(f"Failed to restart monitor for user {user_id}: {e}")
                
                # Log status
                if users_to_start or users_to_stop:
                    logger.debug(
                        f"📊 WebSocket monitors: {len(get_active_monitors())} active, "
                        f"{len(users_needing_monitors)} needed"
                    )
            
            except Exception as e:
                logger.error(f"Error in WebSocket monitor management: {e}")
            
            # Check again in 60 seconds
            await asyncio.sleep(60)
    
    except asyncio.CancelledError:
        # Clean shutdown - stop all monitors
        logger.debug("🛑 Stopping all WebSocket monitors...")
        active_monitors = get_active_monitors()
        for user_id in list(active_monitors.keys()):
            try:
                await stop_monitor_for_user(user_id)
            except Exception as e:
                logger.error(f"Error stopping monitor for user {user_id}: {e}")
        
        logger.debug("✅ All WebSocket monitors stopped")
        raise


async def start_monitor_immediately(user_id: str):
    """Start a monitor for a specific user immediately.
    
    This can be called when a user adds their first tracked note,
    instead of waiting for the next periodic check.
    
    Args:
        user_id: LNbits user ID
    """
    try:
        logger.debug(f"▶️  Starting immediate monitor for user {user_id}")
        await start_monitor_for_user(user_id)
    except Exception as e:
        logger.error(f"Failed to start immediate monitor for user {user_id}: {e}")


async def stop_monitor_immediately(user_id: str):
    """Stop a monitor for a specific user immediately.
    
    This can be called when a user removes all tracked notes,
    instead of waiting for the next periodic check.
    
    Args:
        user_id: LNbits user ID
    """
    try:
        logger.debug(f"⏹️  Stopping immediate monitor for user {user_id}")
        await stop_monitor_for_user(user_id)
    except Exception as e:
        logger.error(f"Failed to stop immediate monitor for user {user_id}: {e}")
