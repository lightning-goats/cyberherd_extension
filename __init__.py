"""Cyberherd LNbits extension.

Nostr-based event monitoring for zaps, reposts, and reactions.
"""

import asyncio
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

from fastapi import APIRouter, Request
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles
from lnbits.tasks import create_permanent_unique_task

# Keep a reference to the db module (which provides init_db) so
# `init_extension` can call the module-level init helper when needed.
from . import db as _db

# Export the Database instance at package level so the core migration loader
# can call `package.db.connect()`. `crud.py` already creates the Database
# as `db = Database("ext_cyberherd")` so import and expose it here.
from .crud import db
from . import crud
from .setup import register_services
from .tasks import cyberherd_tasks, start_invoice_listener
from .services.subscriptions import cyberherd_subscription_handler
from .services.note_metadata import apply_event_address
from .utils.common import extract_t_tags_from_event, utc_now

# Import zap monitor for initialization
from .services.zap_monitor import get_zap_monitor
from .models import CyberherdMember

# Define the extension router and include both web UI and API subrouters
cyberherd_ext: APIRouter = APIRouter(prefix="/cyberherd", tags=["cyberherd"])

# Keep track of background tasks
scheduled_tasks: list = []


def cyberherd_stop():
    """Stop all cyberherd background tasks."""
    for task in scheduled_tasks:
        try:
            task.cancel()
        except Exception as ex:
            logger.warning(f"Error cancelling task: {ex}")


def cyberherd_start():
    """Start cyberherd background tasks using LNbits task system."""
    logger.info("🚀 Cyberherd: Starting background tasks...")
    
    # Create permanent task for subscription handler (like nwcprovider does)
    task = create_permanent_unique_task("ext_cyberherd_subscriptions", cyberherd_subscription_handler)
    scheduled_tasks.append(task)
    logger.info(f"✅ Cyberherd: Subscription handler task created: {task}")
    
    # Create permanent task for WebSocket monitor management
    try:
        async def _start_websocket_monitors():
            try:
                from .services.websocket_monitor_tasks import handle_websocket_monitors
                # Get app reference from LNbits - we need it for event processing
                # For now, pass None and monitors will work without app-specific features
                await handle_websocket_monitors(None)
            except asyncio.CancelledError:
                logger.info("Cyberherd: WebSocket monitor task cancelled")
                raise
            except Exception as e:
                logger.error(f"Cyberherd: WebSocket monitor task error: {e}", exc_info=True)
        
        ws_task = create_permanent_unique_task("ext_cyberherd_websocket_monitors", _start_websocket_monitors)
        scheduled_tasks.append(ws_task)
        logger.info(f"✅ Cyberherd: WebSocket monitor task created: {ws_task}")
    except Exception as e:
        logger.error(f"Cyberherd: Failed to create WebSocket monitor task: {e}", exc_info=True)

    # Ensure the invoice listener is active so realtime payments feed zap processing
    try:
        start_invoice_listener()
        logger.info("✅ Cyberherd: Invoice listener task registered")
    except Exception as e:
        logger.error(f"Cyberherd: Failed to start invoice listener: {e}", exc_info=True)


# Match standard pattern (splitpayments): directly include routers without alias.
try:  # web UI router
    from .views import router as cyberherd_web_router  # type: ignore
    cyberherd_ext.include_router(cyberherd_web_router)
except Exception as e:  # pragma: no cover
    logger.warning(f"Cyberherd: web router import failed: {e}")

try:  # API router
    from .views_api import cyberherd_api_router  # type: ignore
    cyberherd_ext.include_router(cyberherd_api_router)
except Exception as e:  # pragma: no cover
    logger.warning(f"Cyberherd: API router import failed: {e}")

@cyberherd_ext.get("/api/v1/debug_routes")
async def _debug_routes(request: Request):  # pragma: no cover - diagnostic
    """List registered /cyberherd routes (lightweight, no imports).

    This route intentionally avoids importing internal modules so it can be
    relied on even if other extension modules fail to import.
    """
    try:
        app = request.app
        paths = []
        for r in getattr(app, 'routes', []):
            try:
                rp = getattr(r, 'path', None)
                if isinstance(rp, str) and rp.startswith('/cyberherd'):
                    methods = sorted(list(getattr(r, 'methods', []) or []))
                    paths.append({"path": rp, "methods": methods})
            except Exception:
                pass
        return {"routes": paths}
    except Exception as e:
        return {"error": str(e)}


cyberherd_static_files = [
    {"path": "/cyberherd/static", "name": "cyberherd_static"},
]


class CyberHerdStaticFiles(StaticFiles):
    """Static file handler that special-cases leaderboard deep links."""

    def __init__(self, directory: str):
        super().__init__(directory=directory, check_dir=True)
        self._leaderboard_file = os.path.join(directory, "leaderboard.html")

    async def get_response(self, path, scope):
        normalized = (path or "").strip("/")
        if normalized in {"leaderboard", "leaderboard.html"} or normalized.startswith("leaderboard/"):
            if self._leaderboard_file and os.path.exists(self._leaderboard_file):
                return FileResponse(self._leaderboard_file)
        return await super().get_response(path, scope)

# Export an APIRouter named <ext_id>_ext so the LNbits core loader can find it
# (register_ext_routes looks for a symbol named f"{ext.code}_ext").

# Deprecated FastAPI on_event startup handler removed; lifecycle handled in
# init_extension via LNbits core hook.


def init_extension(app):
    """LNbits extension initialization hook."""
    try:
        logger.info("=" * 50)
        logger.info("CYBERHERD: init_extension STARTING")
        logger.info("=" * 50)
    except Exception:
        print("CYBERHERD: init_extension called (logger failed)")
    
    _init_db(app)
    _attach_metadata(app)
    _mount_api_routes(app)
    _mount_ui(app)
    _register_services(app)
    # Schedule midnight reset recurring task
    try:
        cyberherd_tasks(app)
    except Exception as e:
        logger.warning(f"Cyberherd tasks init failed: {e}")
    
    # Register invoice listener so realtime zap detection starts immediately
    try:
        start_invoice_listener(app)
        logger.info("Cyberherd: invoice listener registered during init")
    except Exception as e:
        logger.warning(f"Cyberherd: failed to register invoice listener: {e}")
    
    # WebSocket-based event monitoring is started via cyberherd_start() using create_permanent_unique_task
    # (see cyberherd_start() function above)
    
    # Per-user zap monitors are instantiated lazily when settings accessed; removed legacy singleton.

    # Start zap monitoring for each user with tracking enabled. Fire-and-forget.
    try:
        import asyncio
        async def _run_multi_start():
            try:
                # Wait for nostrclient to initialize relays first
                # This prevents race condition where monitors start before relays are ready
                logger.info("Cyberherd: waiting for nostrclient relay initialization...")
                
                # Use the proper relay readiness helper
                from .services.relay_readiness import wait_for_relays_ready
                relay_status = await wait_for_relays_ready(
                    max_wait_seconds=30.0,
                    check_interval=2.0,
                    min_connected_relays=1
                )
                
                if not relay_status['ready']:
                    # Log detailed status but proceed anyway (non-fatal)
                    logger.warning(
                        f"Cyberherd: Proceeding without confirmed relay readiness | "
                        f"Status: {relay_status['relay_count']} configured, "
                        f"{relay_status['connected_count']} connected, waited {relay_status['waited_seconds']:.1f}s"
                    )
                
                # Now start monitors
                from .crud import db as _cdb, get_settings
                from .utils.common import is_extension_enabled_for_user
                try:
                    rows = await _cdb.fetchall("SELECT user_id FROM cyberherd.settings WHERE zap_tracking_enabled = 1")
                except Exception as db_err:
                    # Tables might not exist yet if migrations haven't run
                    logger.info(f"Cyberherd: skipping zap monitor startup (init_extension), database not ready: {db_err}")
                    return
                started = 0
                for r in rows or []:
                    uid = r.get('user_id')
                    if not uid:
                        continue
                    
                    # Check if user has cyberherd extension enabled
                    if not await is_extension_enabled_for_user(uid):
                        continue
                    
                    try:
                        s = await get_settings(uid)
                    except Exception:
                        continue
                    if not (getattr(s, 'herd_wallet', None) and getattr(s, 'zap_tracking_enabled', False)) and not (getattr(s, 'zap_wallet', None) and getattr(s, 'zap_tracking_enabled', False)):
                        continue
                    try:
                        from .services.zap_monitor import get_zap_monitor
                        zm = get_zap_monitor(app=app, db=_cdb, user_id=uid)
                        await zm.start_monitoring()
                        started += 1
                        logger.info(f"Cyberherd: started zap monitor user={uid[:12]} mode={zm.mode}")
                    except Exception as ie:
                        logger.warning(f"Cyberherd: failed starting zap monitor for user {uid}: {ie}")
                if started > 0:
                    logger.info(f"Cyberherd: zap monitor startup complete - {started} started")
            except Exception as e:  # pragma: no cover
                logger.warning(f"Cyberherd multi zap monitoring init failed: {e}")
        asyncio.create_task(_run_multi_start())
    except Exception as e:  # pragma: no cover
        logger.warning(f"Cyberherd zap monitoring scheduling failed: {e}")

    # Warm start: prefetch today's notes for each user and trigger zap recovery
    try:
        import asyncio
        async def _run_warm():
            try:
                logger.info("Cyberherd: warm start task executing...")
                await _warm_start_today_cache_and_recovery(app)
                logger.info("Cyberherd: warm start task completed")
            except Exception as e:  # pragma: no cover
                logger.warning(f"Cyberherd warm start inner error: {e}")
        logger.info("Cyberherd: scheduling warm start task")
        asyncio.create_task(_run_warm())
        logger.info("Cyberherd: warm start task scheduled")
    except Exception as e:
        logger.warning(f"Cyberherd warm start failed to schedule: {e}")
    
    logger.info("=" * 50)
    logger.info("CYBERHERD: init_extension COMPLETED")
    logger.info("=" * 50)


def _init_db(app):
    try:
        _db.init_db(app)
    except Exception as e:
        logger.warning(e)


def _attach_metadata(app):
    app.state = getattr(app, "state", app)
    app.state.cyberherd = getattr(app.state, "cyberherd", {})
    app.state.cyberherd["init_done"] = True


def _mount_api_routes(app):
    # Try to include the API router into the extension container. This may
    # fail during early import if views_api has unresolved dependencies; log
    # detailed traceback so startup logs surface the real error instead of
    # only a generic warning emitted at module import time.
    try:
        from .views_api import cyberherd_api_router  # type: ignore
        try:
            cyberherd_ext.include_router(cyberherd_api_router)
            logger.info("Cyberherd: API router included into cyberherd_ext")
        except Exception as ie:
            logger.exception(f"Cyberherd: failed to include API router: {ie}")
    except Exception as e:
        # Provide full traceback in debug logs for diagnosis
        try:
            import traceback

            tb = traceback.format_exc()
            logger.warning(f"Cyberherd: API router import failed during mount: {e}")
            logger.debug(f"Cyberherd: API router import traceback:\n{tb}")
        except Exception:
            logger.warning(f"Cyberherd: API router import failed: {e}")


def _mount_ui(app):
    try:
        # Attach the web UI router to the extension container lazily
        try:
            from .views import router as cyberherd_web_router  # type: ignore
            cyberherd_ext.include_router(cyberherd_web_router)
        except Exception as e:
            logger.warning(f"Cyberherd: failed to import web router: {e}")
            # Provide a minimal fallback index to avoid 404 on /cyberherd/
            try:
                async def _fallback_index():
                    return {"cyberherd": "ok"}
                cyberherd_ext.add_api_route("/", _fallback_index, methods=["GET"])
            except Exception:
                pass
        # Mount extension container and static assets
        app.include_router(cyberherd_ext)
        static_path = os.path.join(os.path.dirname(__file__), "static")
        try:
            app.mount(
                "/cyberherd/static",
                CyberHerdStaticFiles(directory=static_path),
                name="cyberherd_static",
            )
        except Exception as se:  # pragma: no cover
            logger.warning(f"Cyberherd: static mount failed: {se}")
        tile_file = os.path.join(static_path, "tile.png")
        if os.path.exists(tile_file):
            try:
                app.add_route(
                    "/cyberherd/tile.png",
                    lambda _: FileResponse(tile_file),
                    methods=["GET"],
                )
            except Exception as te:  # pragma: no cover
                logger.debug(f"Cyberherd: tile route add failed: {te}")
    except Exception as e:
        logger.warning(f"Cyberherd extension UI registration failed: {e}")


def _register_services(app):
    try:
        register_services(app)
    except Exception as e:
        logger.warning(f"Cyberherd background services failed to register: {e}")


async def _start_zap_monitoring_if_enabled(app):
    """Start zap monitoring service if it's enabled in settings."""
    try:
        logger.info("Cyberherd: Starting zap monitoring check...")
        # Prefer a user-scoped settings row with tracking enabled; fall back to global
        settings = await crud.get_settings()
        logger.info(f"Cyberherd: Got settings: {settings is not None}")
        user_id = None
        if settings:
            logger.info(f"Cyberherd: Zap tracking enabled: {getattr(settings, 'zap_tracking_enabled', False)}")
        if not (settings and getattr(settings, 'zap_tracking_enabled', False)):
            try:
                from lnbits.db import Database
                db = Database("ext_cyberherd")
                row = await db.fetchone(
                    "SELECT user_id FROM cyberherd.settings WHERE zap_tracking_enabled = 1 LIMIT 1"
                )
                if row and row.get('user_id'):
                    user_id = row['user_id']
                    settings = await crud.get_settings(user_id)
                    logger.info(f"Cyberherd: Found user settings for user_id: {user_id}")
            except Exception as e:
                logger.warning(f"Cyberherd: Error getting user settings: {e}")

        if settings and getattr(settings, 'zap_tracking_enabled', False):
            zap_monitor = getattr(app.state, 'cyberherd_zap_monitor', None)
            logger.info(f"Cyberherd: Zap monitor in app state: {zap_monitor is not None}")
            if zap_monitor:
                if user_id:
                    zap_monitor.user_id = user_id
                await zap_monitor.start_monitoring()
                logger.info("Cyberherd zap monitoring started")
            else:
                logger.warning("Zap monitor service not found in app state")
        else:
            logger.info("Cyberherd zap monitoring disabled in settings")
    except Exception as e:
        logger.warning(f"Failed to start zap monitoring: {e}")


async def _warm_start_today_cache_and_recovery(app):
    """Populate today's note IDs per user into cache for immediate UI display.
    
    Note: Automatic recovery removed - users can manually trigger recovery via UI button.
    """
    try:
        from datetime import datetime, timezone
        from . import crud
        from .views_api import _get_cached_effective_pubkey, _cache_notes
        from .services import nostr_helpers
        from .services.time_utils import get_day_boundaries_utc
        from .utils.common import is_extension_enabled_for_user

        logger.info("Cyberherd warm start: begin prefetch")
        try:
            rows = await crud.db.fetchall("SELECT * FROM cyberherd.settings")
            logger.info(f"Cyberherd warm start: database query succeeded, got {len(rows or [])} rows")
        except Exception as db_err:
            logger.warning(f"Cyberherd warm start: database query failed: {db_err}")
            rows = []
        logger.info(f"Cyberherd warm start: found {len(rows)} settings rows")

        # Get day boundaries using UTC-first approach for consistency
        # Cache key uses UTC date, but 'since' filter uses local_since_ts for user's "today"
        boundaries = get_day_boundaries_utc()
        day_key = boundaries.utc_day_str
        since = int(boundaries.local_since_ts)  # Use local midnight for user's "today"
        until = int(boundaries.local_until_ts)

        try:
            from .services import nostr_helpers
            relay_info = nostr_helpers.get_relay_info()
            logger.info(f"Cyberherd warm start: nostrclient relays={relay_info.get('relay_count', 0)} connected={relay_info.get('connected_count', 0)}")
        except Exception:
            pass

        processed = 0
        for r in rows:
            try:
                uid = r.get("user_id")
                
                # Check if user has cyberherd extension enabled
                if not await is_extension_enabled_for_user(uid):
                    continue

                s = await crud.get_settings(uid)
                tags = [t.lstrip('#').lower() for t in (getattr(s, 'tracked_tags', []) or [])]
                eff_pub = _get_cached_effective_pubkey(s)
                logger.info(f"Cyberherd warm start: user={uid} tags={tags} eff_pub_present={bool(eff_pub)}")
                if tags and eff_pub:
                    try:
                        # Include kind 1 (notes) and kind 30311 (long-form content)
                        events = await nostr_helpers.query_events({"kinds": [1, 30311], "authors": [eff_pub], "since": since, "until": until}, limit=100, timeout=8.0)
                        ids = []
                        timestamps = {}  # Store note_id -> created_at timestamp
                        addresses = dict(getattr(s, 'tracked_event_addresses', {}) or {})
                        for ev in events or []:
                            try:
                                ca = int(ev.get('created_at') or 0)
                            except Exception:
                                ca = 0
                            if not (since <= ca < until):
                                continue
                            tag_values = extract_t_tags_from_event(ev)
                            content = (ev.get('content') or '').lower()
                            if (set(tags) & tag_values) or any(f"#{tv}" in content for tv in tags):
                                if isinstance(ev.get('id'), str):
                                    note_id = ev['id']
                                    ids.append(note_id)
                                    timestamps[note_id] = ca  # Store timestamp for this note
                                    apply_event_address(addresses, ev)
                        tagset = tuple(sorted(tags))
                        _cache_notes(app, (day_key, uid, eff_pub, tagset), ids)
                        _cache_notes(app, (day_key, None, eff_pub, tagset), ids)
                        
                        # Update settings with tracked_event_ids (current day only)
                        if ids:
                            try:
                                s.tracked_event_ids = ids
                                s.tracked_event_timestamps = timestamps
                                s.tracked_event_addresses = addresses
                                await crud.upsert_settings(s, uid)
                                logger.info(f"Warm start: updated tracked_event_ids for user {uid}: {len(ids)} events")
                                processed += 1
                            except Exception as e:
                                logger.error(f"Warm start: failed to update tracked_event_ids for user {uid}: {e}")
                    except Exception as e:
                        logger.debug(f"Warm start: note prefetch failed user={uid}: {e}")
            except Exception as e:
                logger.debug(f"Warm start: settings row error: {e}")
        
        if processed > 0:
            logger.info(f"Cyberherd warm start: complete - {processed} users processed")
    except Exception as e:
        logger.warning(f"Warm start error: {e}")


__all__ = ["cyberherd_ext", "db", "init_extension", "cyberherd_start", "cyberherd_stop"]
