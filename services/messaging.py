"""Messaging adapter for the Cyberherd extension.

This module keeps CyberHerd-specific message templates and payload builders,
but delegates generic functionality (websocket broadcast and nostr posting)
to the cyberherd_messaging extension services.
"""

from datetime import datetime, timezone
import random
import time
from typing import Optional, Iterable, Dict, Tuple, List, Any, TYPE_CHECKING

from loguru import logger

# Conditional imports for cyberherd_messaging extension
try:
    from lnbits.extensions.cyberherd_messaging import crud as _msg_crud
    from lnbits.extensions.cyberherd_messaging import services as _msg
    _messaging_available = True
except ImportError:
    _msg_crud = None
    _msg = None
    _messaging_available = False

# Log the messaging availability status
if not _messaging_available:
    logger.warning("cyberherd_messaging extension not available, messaging features will be disabled")


def _format_with_safe_map(template_str: str, values: dict | None) -> str:
    """Safely format a template string using a dict that returns {key} for missing keys.

    Keeps formatting attempts safe and centralizes the SafeDict implementation to
    avoid repeated nested class definitions which can confuse linters/typecheckers.
    """
    class _SafeDict(dict):
        def __missing__(self, k):
            return "{" + str(k) + "}"

    try:
        return str(template_str).format_map(_SafeDict(values or {}))
    except Exception:
        return str(template_str)


if TYPE_CHECKING:  # pragma: no cover - optional import for typing only
    try:
        from lnbits.extensions.cyberherd_messaging.message_builder import MessageBundle
    except Exception:  # pragma: no cover
        MessageBundle = Any  # type: ignore


_MEMBERSHIP_EVENTS = {"cyber_herd", "new_member"}


def _coerce_int(value: Any, default: int = 0) -> int:
    """Coerce arbitrary numeric-like values into integers."""
    if value is None:
        return default
    try:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip()
        if not text:
            return default
        return int(float(text))
    except Exception:
        return default


def _format_spots_suffix(spots_remaining: int) -> str:
    if spots_remaining > 1:
        return f"⚡ {spots_remaining} more spots available. ⚡"
    if spots_remaining == 1:
        return "⚡ 1 more spot available. ⚡"
    return ""


def _build_membership_context(values: dict[str, Any] | None) -> Optional[dict[str, Any]]:
    if not isinstance(values, dict):
        return None

    ch_item = dict(values.get("cyber_herd_item") or {})

    display_name = (
        values.get("member_display_name")
        or values.get("display_name")
        or values.get("name")
        or ch_item.get("display_name")
        or "Anon"
    )
    ch_item.setdefault("display_name", display_name)
    ch_item.setdefault("pubkey", values.get("member_pubkey") or values.get("pubkey") or ch_item.get("pubkey"))
    ch_item.setdefault("nprofile", values.get("member_nprofile") or values.get("nprofile") or ch_item.get("nprofile"))
    ch_item.setdefault("event_id", values.get("event_id") or values.get("note_id") or ch_item.get("event_id"))

    amount_candidate = ch_item.get("amount")
    if amount_candidate is None:
        for key in ("initial_amount", "new_amount", "amount", "increase_amount"):
            if values.get(key) is not None:
                amount_candidate = values.get(key)
                break
    ch_item["amount"] = _coerce_int(amount_candidate, 0)

    if "headbutt_info" in values and values.get("headbutt_info"):
        try:
            if isinstance(values.get("headbutt_info"), dict):
                ch_item.setdefault("headbutt_info", dict(values.get("headbutt_info")))
            else:
                ch_item.setdefault("headbutt_info", values.get("headbutt_info"))
        except Exception:
            ch_item.setdefault("headbutt_info", values.get("headbutt_info"))

    context = {
        "cyber_herd_item": ch_item,
        "new_amount": _coerce_int(
            values.get("new_amount", values.get("initial_amount", ch_item.get("amount", 0))), 0
        ),
        "difference": _coerce_int(values.get("difference"), 0),
        "spots_remaining": _coerce_int(values.get("spots_remaining", values.get("_spots_remaining")), 0),
        "relays": values.get("relays"),
    }
    return context


def _prepare_bundle_args(event_type: str, values: dict[str, Any] | None) -> dict[str, Any] | None:
    """Convert loosely-structured template values into message bundle kwargs."""
    if not isinstance(values, dict):
        return None

    ch_item = dict(values.get("cyber_herd_item") or {})

    def _setdefault(item_key: str, *source_keys: str, transform=None) -> None:
        if item_key in ch_item:
            return
        for key in source_keys:
            if key in values and values[key] is not None:
                value = values[key]
                ch_item[item_key] = transform(value) if transform else value
                return

    display_name = (
        ch_item.get("display_name")
        or values.get("member_display_name")
        or values.get("display_name")
        or values.get("member_name")
        or values.get("name")
        or "Anon"
    )
    ch_item.setdefault("display_name", display_name)
    _setdefault("pubkey", "member_pubkey", "pubkey")
    _setdefault("nprofile", "member_nprofile", "nprofile")
    _setdefault("event_id", "event_id", "note_id")
    _setdefault("picture", "picture", "imageUrl", "image_url")

    if "headbutt_info" in values and values.get("headbutt_info") and "headbutt_info" not in ch_item:
        try:
            if isinstance(values.get("headbutt_info"), dict):
                ch_item["headbutt_info"] = dict(values.get("headbutt_info"))
            else:
                ch_item["headbutt_info"] = values.get("headbutt_info")
        except Exception:
            ch_item["headbutt_info"] = values.get("headbutt_info")

    # Event-specific enrichment
    if event_type in {"headbutt_success", "headbutt_failure"}:
        for key in (
            "attacker_name",
            "attacker_amount",
            "attacker_pubkey",
            "attacker_nprofile",
            "victim_name",
            "victim_amount",
            "victim_pubkey",
            "victim_nprofile",
            "event_id",
        ):
            _setdefault(key, key)

        if event_type == "headbutt_success":
            if "next_headbutt_info" not in ch_item and isinstance(values.get("next_headbutt_info"), dict):
                ch_item["next_headbutt_info"] = dict(values["next_headbutt_info"])
        else:  # headbutt_failure specific fields
            _setdefault("required_amount", "required_amount", "required_sats")
            _setdefault("required_sats", "required_sats", "required_amount")

    if event_type == "headbutt_info":
        _setdefault("required_sats", "required_sats", "required_amount")
        _setdefault("victim_name", "victim_name")
        _setdefault("victim_pubkey", "victim_pubkey")
        _setdefault("victim_nprofile", "victim_nprofile")
        _setdefault("event_id", "event_id", "note_id")

    if event_type == "cyber_herd_treats":
        _setdefault("amount", "amount", "new_amount")
        _setdefault("pubkey", "member_pubkey", "pubkey")
        _setdefault("nprofile", "member_nprofile", "nprofile")

    if event_type == "member_increase":
        _setdefault("new_zap_amount", "increase_amount")
        _setdefault("amount", "new_total", "amount", "new_amount")
        _setdefault("pubkey", "member_pubkey", "pubkey")
        _setdefault("nprofile", "member_nprofile", "nprofile")

    if event_type in _MEMBERSHIP_EVENTS:
        _setdefault("amount", "amount", "new_amount", "initial_amount", "increase_amount")

    # Clean coercions for numeric fields
    for key in (
        "amount",
        "attacker_amount",
        "victim_amount",
        "required_amount",
        "required_sats",
        "new_zap_amount",
    ):
        if key in ch_item and ch_item[key] is not None:
            ch_item[key] = _coerce_int(ch_item[key], 0)

    # Drop falsy values to keep payload compact
    ch_item_clean = {k: v for k, v in ch_item.items() if v is not None}

    # Derive primitive fields for bundle call
    new_amount_candidate = None
    for key in ("new_amount", "initial_amount", "increase_amount", "new_total", "amount"):
        if values.get(key) is not None:
            new_amount_candidate = values.get(key)
            break
    if new_amount_candidate is None and "amount" in ch_item_clean:
        new_amount_candidate = ch_item_clean.get("amount")

    bundle_args: dict[str, Any] = {
        "new_amount": _coerce_int(new_amount_candidate, 0),
        "difference": _coerce_int(values.get("difference"), 0),
        "spots_remaining": _coerce_int(
            values.get("spots_remaining", values.get("_spots_remaining")), 0
        ),
        "relays": values.get("relays"),
    }

    if ch_item_clean:
        bundle_args["cyber_herd_item"] = ch_item_clean

    return bundle_args


async def _build_message_bundle_from_values(
    event_type: str,
    values: dict[str, Any] | None,
    *,
    reply_to_30311_event: str | None = None,
    reply_to_30311_a_tag: str | None = None,
) -> "MessageBundle | None":
    """Try to build a MessageBundle using the messaging extension defaults."""
    if (
        not _messaging_available
        or _msg is None
        or not hasattr(_msg, "build_message_bundle")
    ):
        return None

    bundle_args = _prepare_bundle_args(event_type, values)
    if not bundle_args:
        return None

    try:
        return await _msg.build_message_bundle(
            event_type,
            reply_to_30311_event=reply_to_30311_event,
            reply_to_30311_a_tag=reply_to_30311_a_tag,
            **bundle_args,
        )
    except Exception as exc:
        logger.debug("cyberherd: failed to build message bundle for %s: %s", event_type, exc)
        return None


async def _append_membership_extras(
    content: str,
    event_type: str,
    values: dict[str, Any] | None,
    *,
    reply_to_30311_event: str | None = None,
    reply_to_30311_a_tag: str | None = None,
) -> str:
    if event_type not in _MEMBERSHIP_EVENTS:
        return content

    context = _build_membership_context(values)
    if not context:
        return content

    extras = ""
    if _messaging_available and _msg is not None and hasattr(_msg, "build_message_bundle"):
        try:
            bundle = await _msg.build_message_bundle(
                event_type,
                new_amount=context["new_amount"],
                difference=context["difference"],
                cyber_herd_item=context["cyber_herd_item"],
                spots_remaining=context["spots_remaining"],
                relays=context["relays"],
                reply_to_30311_event=reply_to_30311_event,
                reply_to_30311_a_tag=reply_to_30311_a_tag,
            )
            extras = (bundle.spots_info or "") + (bundle.headbutt_text or "")
        except Exception as exc:
            logger.debug("cyberherd: failed to build membership extras: %s", exc)
            extras = _format_spots_suffix(context["spots_remaining"])
    else:
        extras = _format_spots_suffix(context["spots_remaining"])

    if extras:
        return (content or "") + extras
    return content


async def send_to_websocket_clients(item_id: str, message: dict, websocket_topic: str = "cyberherd"):
    """Delegate to generic websocket broadcast service.
    
    Args:
        item_id: Legacy parameter, now ignored in favor of websocket_topic
        message: The message payload to broadcast
        websocket_topic: WebSocket topic/wallet ID to broadcast to (default: "cyberherd")
    """
    if not _messaging_available or _msg is None:
        logger.debug("Messaging not available, skipping websocket broadcast")
        return False
    
    # Use websocket_topic instead of item_id for routing
    topic = websocket_topic or "cyberherd"
    
    # Try to use the helper function from cyberherd_messaging
    if hasattr(_msg, 'send_to_websocket_clients'):
        return await _msg.send_to_websocket_clients(topic, message)
    
    # Fallback: call websocket_updater directly if helper is missing
    logger.warning(
        "cyberherd_messaging.services.send_to_websocket_clients not found, "
        "using direct websocket_updater fallback"
    )
    try:
        import json
        from lnbits.core.services.websockets import websocket_updater
        payload = json.dumps(message)
        await websocket_updater(topic, payload)
        return True
    except Exception as e:
        logger.error(f"Fallback WebSocket broadcast failed: {e}")
        return False


async def send_cyberherd_update(
    newest_pubkey: Optional[str] = None,
    db=None,
    event_id: Optional[str] = None,
    note_id: Optional[str] = None,
    p_tags: list[str] | None = None,
    websocket_topic: str = "cyberherd",
):
    """Build a lightweight cyberherd update payload and broadcast it.

    If `ln_post_event` is available we can also post a nostr note; otherwise
    we only send websocket updates to clients.
    
    Args:
        newest_pubkey: Public key of the newest member
        db: Database connection
        event_id: Optional event ID
        note_id: Optional note ID
        p_tags: Optional p_tags list
        websocket_topic: WebSocket topic/wallet ID to broadcast to (default: "cyberherd")
    """
    # Attempt to build a payload from the DB if provided
    payload = {"type": "cyberherd_update", "newest": newest_pubkey}
    try:
        if db is not None:
            rows = await db.fetch_all(
                (
                    "SELECT pubkey, display_name, amount, is_active, picture, nprofile "
                    "FROM cyberherd.cyber_herd ORDER BY amount DESC LIMIT 10"
                ),
            )
            members: list[dict[str, Any]] = []
            for row in rows:
                member_dict = dict(row)
                picture = member_dict.get("picture")
                if picture:
                    member_dict.setdefault("imageUrl", picture)
                members.append(member_dict)
            payload["members"] = members
    except Exception as e:
        # Non-fatal; continue with minimal payload
        logger.warning(e)

    await send_to_websocket_clients("cyberherd", payload, websocket_topic=websocket_topic)

    # Do not post a generic nostr note here; headbutt success/failure flows
    # already generate templated messages and publish them as threaded replies.
    logger.info(
        f"cyberherd: websocket update broadcasted to topic={websocket_topic}; skipping generic nostr post (newest={newest_pubkey})"
    )


async def make_messages(event_type: str, **kwargs) -> dict:
    """Create basic message content for nostr posting and websocket payloads.

    This function now uses shared templates from cyberherd_messaging when available,
    falling back to local content for backward compatibility.
    """

    bundle: "MessageBundle | None" = await _build_message_bundle_from_values(
        event_type,
        kwargs,
        reply_to_30311_event=kwargs.get("reply_to_30311_event"),
        reply_to_30311_a_tag=kwargs.get("reply_to_30311_a_tag"),
    )
    if bundle:
        content = bundle.nostr_content or ""
        payload: dict[str, Any] = {"type": event_type, **kwargs}
        payload["message"] = bundle.websocket_content or content
        if bundle.goat_data:
            payload["goat_data"] = bundle.goat_data
        if bundle.spots_info:
            payload["spots_info"] = bundle.spots_info
        if bundle.headbutt_text:
            payload["headbutt_text"] = bundle.headbutt_text
        try:
            from time import time as _time
            _LAST_MAKE_CALLS[(kwargs.get("owner_user_id"), event_type)] = (str(content or ""), float(_time()))
        except Exception:
            pass
        return {"content": content, "payload": payload}

    # Helper to build display names preferring nostr nprofiles
    def _format_name(role: str) -> str:
        try:
            nprof = kwargs.get(f"{role}_nprofile")
            if nprof:
                return f"nostr:{nprof}"
            # explicit name overrides
            name = (
                kwargs.get(f"{role}_name")
                or kwargs.get(role)
                or kwargs.get(f"{role}_display_name")
            )
            if name:
                return str(name)
            # fallback to npub if we have a pubkey
            pk = kwargs.get(f"{role}_pubkey")
            if isinstance(pk, str) and len(pk) == 64:
                try:
                    from lnbits.utils.nostr import hex_to_npub

                    return hex_to_npub(pk)
                except Exception:
                    return pk[:8]
        except Exception:
            pass
        return "Anon"

    # Map event types to shared template categories
    # Category names must match those exposed by cyberherd_messaging extension
    template_mapping = {
        "headbutt_success": "headbutt_success",
        "headbutt_failure": "headbutt_failure",
        "headbutt_info": "headbutt_info",
        # New/join events use the shared join category
        "cyber_herd": "cyber_herd",
        "new_member": "cyber_herd",
        # Member amount increase
        "member_increase": "member_increase",
        # Repost / reaction specific categories (use exact categories available in cyberherd_messaging)
        "kind_6_repost": "kind_6_repost",
        "kind_7_reaction": "kind_7_reaction",
        "zapper_displaces_kind_6": "zapper_displaces_kind_6",
        "zapper_displaces_kind_7": "zapper_displaces_kind_7",
        "kind_6_headbutt_failure": "kind_6_headbutt_failure",
        "kind_7_headbutt_failure": "kind_7_headbutt_failure",
        # Additional mappings for zap-specific messages
        "variations": "variations",
    }

    # Try to get owner_user_id from kwargs
    owner_user_id = kwargs.get("owner_user_id")
    logger.debug(f"make_messages: event_type={event_type}, owner_user_id={owner_user_id}, messaging_available={_messaging_available}")
    
    # Note: owner_user_id should be passed in from calling code
    # It cannot be derived here since we're not in a request context

    # If we have an owner_user_id, try to use shared templates
    if owner_user_id and event_type in template_mapping and _messaging_available and _msg_crud is not None:
        try:
            # Use the already imported _msg_crud
            category = template_mapping[event_type]
            logger.debug(f"make_messages: trying to get template for user={owner_user_id}, category={category}, key=0")
            # Get template directly since we're in async context
            template = await _msg_crud.get_message_template(owner_user_id, category, "0")
            
            if template and template.content:
                logger.info(f"make_messages: found template for {event_type}, content length: {len(template.content)}")
                # Format the template with provided values
                content = template.content
                # Safely format using the centralized helper
                try:
                    content = _format_with_safe_map(content, kwargs)
                except Exception as e:
                    logger.warning(f"Template formatting failed: {e}")
                
                # Create payload based on event type
                payload = {"type": event_type, **kwargs}
                # Record last make_messages call for optional test assertions
                try:
                    from time import time as _time
                    _LAST_MAKE_CALLS[(owner_user_id, event_type)] = (str(content or ""), float(_time()))
                except Exception:
                    pass
                return {"content": content, "payload": payload}
            else:
                logger.debug(f"make_messages: no template found for user={owner_user_id}, category={category}")
        except Exception as e:
            logger.warning(f"Failed to use shared template for {event_type}: {e}")
    else:
        logger.debug(f"make_messages: skipping template lookup - owner_user_id={owner_user_id}, event_in_mapping={event_type in template_mapping}, messaging_available={_messaging_available}")

    # If we didn't find a shared template for the owner, attempt to use the
    # packaged defaults from the cyberherd_messaging extension first. If that
    # isn't available (extension not installed), fall back to the embedded
    # messages_templates module in this extension.
    if not owner_user_id and event_type in template_mapping:
        category = template_mapping[event_type]
        # Try messaging extension packaged defaults with flexible key matching
        try:
            from lnbits.extensions.cyberherd_messaging.defaults import SEED_DEFAULTS as _seed
            # Try exact key, then keys that startwith category, then keys that contain category
            seed_key = None
            if category in _seed:
                seed_key = category
            else:
                for k in _seed.keys():
                    if k.startswith(category):
                        seed_key = k
                        break
            if not seed_key:
                for k in _seed.keys():
                    if category in k:
                        seed_key = k
                        break

            if seed_key:
                logger.debug(f"make_messages: found seed default key='{seed_key}' for category='{category}'")
                tmpl = _seed.get(seed_key)
            else:
                tmpl = None

            if isinstance(tmpl, dict) and tmpl:
                try:
                    raw = random.choice(list(tmpl.values()))
                except Exception:
                    try:
                        raw = next(iter(tmpl.values()))
                    except Exception:
                        raw = str(tmpl)

                if raw is None:
                    raw = ""
                if isinstance(raw, dict):
                    raw = raw.get("content", "")
                raw_str = str(raw)
                # Safe formatting
                content = _format_with_safe_map(raw_str, kwargs)

                payload = {"type": event_type, **kwargs}
                try:
                    from time import time as _time
                    _LAST_MAKE_CALLS[(owner_user_id, event_type)] = (str(content or ""), float(_time()))
                except Exception:
                    pass
                return {"content": content, "payload": payload}
        except Exception:
            # Fall through to internal messages_templates fallback
            pass

        # Last-resort: use local messages_templates (older embedded templates)
        try:
            from . import messages_templates as _local_templates

            # Try direct attribute, then names that startwith or contain the category
            tmpl = getattr(_local_templates, category, None)
            if tmpl is None:
                for name in dir(_local_templates):
                    if name.startswith(category):
                        tmpl = getattr(_local_templates, name, None)
                        if tmpl is not None:
                            logger.debug(f"make_messages: matched local template name='{name}' for category='{category}'")
                            break
            if tmpl is None:
                for name in dir(_local_templates):
                    if category in name:
                        tmpl = getattr(_local_templates, name, None)
                        if tmpl is not None:
                            logger.debug(f"make_messages: matched local template name='{name}' for category='{category}' (contains)')")
                            break

            if isinstance(tmpl, dict) and tmpl:
                try:
                    raw = random.choice(list(tmpl.values()))
                except Exception:
                    try:
                        raw = next(iter(tmpl.values()))
                    except Exception:
                        raw = None

                if raw is None:
                    raw = ""
                if isinstance(raw, dict):
                    raw = raw.get("content", "")
                raw_str = str(raw)
                # Safe formatting
                try:
                    content = _format_with_safe_map(raw_str, kwargs)
                except Exception:
                    content = raw_str

                payload = {"type": event_type, **kwargs}
                try:
                    from time import time as _time
                    _LAST_MAKE_CALLS[(owner_user_id, event_type)] = (str(content or ""), float(_time()))
                except Exception:
                    pass
                return {"content": content, "payload": payload}
        except Exception:
            pass

    # Fallback to local message creation
    if event_type == "headbutt_success":
        attacker_name = kwargs.get("attacker_name") or _format_name("attacker")
        victim_name = kwargs.get("victim_name") or _format_name("victim")
        content = (
            f"⚡headbutt⚡: {attacker_name} has displaced {victim_name}"
            f" ({kwargs.get('attacker_amount')} vs {kwargs.get('victim_amount')})."
        )
        payload = {
            "type": "headbutt_success",
            "attacker": attacker_name,
            "victim": victim_name,
        }
        try:
            from time import time as _time
            _LAST_MAKE_CALLS[(owner_user_id if owner_user_id else None, event_type)] = (str(content or ""), float(_time()))
        except Exception:
            pass
        return {"content": content, "payload": payload}

    # Feeding events removed per updated requirements.

    if event_type == "member_increase":
        member_name = kwargs.get("member_name") or _format_name("member")
        content = (
            f"{member_name} increased by {kwargs.get('increase_amount', 0)} "
            f"to {kwargs.get('new_total', 0)} sats."
        )
        payload = {
            "type": "member_increase",
            "member": member_name,
            "increase_amount": kwargs.get("increase_amount", 0),
            "new_total": kwargs.get("new_total", 0),
        }
        return {"content": content, "payload": payload}

    if event_type == "headbutt_failure":
        # Informational message indicating required sats
        victim_name = kwargs.get("victim_name") or _format_name("victim")
        content = (
            f"Needs {kwargs.get('required_amount')} more sats to beat {victim_name}."
        )
        payload = {
            "type": "headbutt_failure",
            "required_amount": kwargs.get("required_amount"),
            "victim": victim_name,
        }
        return {"content": content, "payload": payload}

    if event_type in ["sats_received", "feeder_triggered", "feeder_trigger_bolt12"]:
        name = kwargs.get("name") or _format_name("member")
        new_amount = kwargs.get("new_amount", 0)
        difference = kwargs.get("difference", 0)
        if event_type == "sats_received":
            content = f"{name} contributed {new_amount} sats. {difference} sats until feeder activation."
        else:
            content = f"Feeder triggered with {new_amount} sats! {name} gets treats."
        payload = {
            "type": event_type,
            "name": name,
            "new_amount": new_amount,
            "difference": difference,
        }
        return {"content": content, "payload": payload}

    if event_type in ["cyber_herd_treats", "feeding_regular", "feeding_bonus", "feeding_remainder", "feeding_fallback"]:
        name = kwargs.get("name") or _format_name("member")
        new_amount = kwargs.get("new_amount", 0)
        content = f"{name} received {new_amount} sats from CyberHerd distribution."
        payload = {
            "type": event_type,
            "name": name,
            "new_amount": new_amount,
        }
        return {"content": content, "payload": payload}

    if event_type in ["cyber_herd_info", "interface_info"]:
        content = kwargs.get("message", "CyberHerd information update.")
        payload = {
            "type": event_type,
            "message": content,
        }
        return {"content": content, "payload": payload}

    if event_type == "herd_reset":
        content = "🐐 The herd has been reset! All goats are back to the starting gate."
        payload = {
            "type": "herd_reset",
            "message": content,
        }
        return {"content": content, "payload": payload}

    if event_type in ["daily_reset", "payment_metrics", "system_status", "weather_status"]:
        content = kwargs.get("message", f"{event_type.replace('_', ' ').title()} update.")
        payload = {
            "type": event_type,
            "message": content,
        }
        return {"content": content, "payload": payload}

    if event_type == "sats_received_zap":
        name = kwargs.get("name") or _format_name("member")
        new_amount = kwargs.get("new_amount", 0)
        content = f"A zap of {new_amount} sats has been received from {name}!"
        payload = {
            "type": event_type,
            "name": name,
            "new_amount": new_amount,
        }
        return {"content": content, "payload": payload}

    # generic fallback - avoid publishing raw stringified dicts
    # Prefer an explicit 'message' value when present, then a friendly name
    # summary. Fall back to JSON if nothing more readable is available.
    content = None
    try:
        if isinstance(kwargs, dict) and kwargs.get("message"):
            content = kwargs.get("message")
        elif isinstance(kwargs, dict) and (kwargs.get("member_name") or kwargs.get("name") or kwargs.get("display_name")):
            name = kwargs.get("member_name") or kwargs.get("name") or kwargs.get("display_name")
            content = f"{event_type.replace('_', ' ').title()}: {name}"
        else:
            import json

            try:
                content = json.dumps(kwargs, ensure_ascii=False)
            except Exception:
                content = str(kwargs)
    except Exception:
        content = str(kwargs)

    content_str = str(content or "")
    content_str = await _append_membership_extras(
        content_str,
        event_type,
        kwargs,
    )
    return {"content": content_str, "payload": {"type": event_type, **kwargs}}


async def publish_shared_template(
    *,
    owner_user_id: str,
    category: str,
    key: str,
    values: dict | None = None,
    e_tags: list[str] | None = None,
    p_tags: list[str] | None = None,
    private_key: str | None = None,
    websocket_topic: str = "cyberherd",
    message_type: str | None = None,
    reply_to_30311_event: str | None = None,
    reply_to_30311_a_tag: str | None = None,
    reply_relay: str | None = None,
) -> bool:
    """Convenience wrapper to render+publish a template from the shared store.

    Returns False if the template doesn't exist or publishing fails.
    
    Args:
        owner_user_id: User ID whose templates to use
        category: Template category (used for template lookup)
        key: Template key
        values: Values to format the template with
        e_tags: Event tags for threading
        p_tags: Pubkey tags
        private_key: Nostr private key for signing
        websocket_topic: WebSocket topic/wallet ID to broadcast to (default: "cyberherd")
        message_type: Semantic event name for websocket (e.g., "headbutt_success"). 
                     If None, uses category. This allows storage categories to differ 
                     from websocket type names.
        reply_to_30311_event: Optional event ID of the kind 30311 note being replied to
        reply_to_30311_a_tag: Optional `a` tag address for the kind 30311 note
    """
    if not _messaging_available or _msg_crud is None or _msg is None:
        logger.debug("Messaging not available, skipping shared template publication")
        return False

    values_dict: dict[str, Any] = dict(values or {})
    semantic_type = message_type or category
    if semantic_type:
        try:
            values_dict.setdefault("_semantic_event_type", str(semantic_type))
        except Exception:
            values_dict["_semantic_event_type"] = semantic_type
    if reply_to_30311_event is not None:
        values_dict.setdefault("reply_to_30311_event", reply_to_30311_event)
    if reply_to_30311_a_tag is not None:
        values_dict.setdefault("reply_to_30311_a_tag", reply_to_30311_a_tag)

    try:
        # quick existence check to avoid confusing logs
        tpl = await _msg_crud.get_message_template(owner_user_id, category, key)
        if not tpl:
            return False
    except Exception:
        # proceed to the renderer which already handles errors/fallbacks
        tpl = None
    # Check if Nostr publishing is enabled; if not, we'll only broadcast to websockets
    nostr_enabled = True
    try:
        nostr_enabled = await _msg.is_nostr_publishing_enabled()
    except Exception:
        nostr_enabled = True

    ok = False
    if nostr_enabled:
        logger.info(f"cyberherd: sending message to cyberherd_messaging extension (category={category}, key={key}, user={owner_user_id})")
        ok = await _msg.render_and_publish_template(
            user_id=owner_user_id,
            category=category,
            key=key,
            values=values_dict,
            e_tags=e_tags,
            p_tags=p_tags,
            private_key=private_key,
            reply_to_30311_event=reply_to_30311_event,
            reply_to_30311_a_tag=reply_to_30311_a_tag,
            reply_relay=reply_relay,
        )
        if ok:
            logger.info(f"cyberherd: message published successfully via cyberherd_messaging extension")
        else:
            logger.warning(f"cyberherd: message failed to publish via cyberherd_messaging extension")
    # If published successfully, mirror to websocket clients for overlays (progress.html)
    broadcasted = False
    if ok or not nostr_enabled:
        content_to_broadcast: str | None = None
        goat_data = None
        extras_needed = False

        def _format_local_content() -> str | None:
            if not tpl or not getattr(tpl, "content", None):
                return None
            try:
                return _format_with_safe_map(str(tpl.content), values_dict or {})
            except Exception:
                try:
                    return str(tpl.content)
                except Exception:
                    return None

        try:
            websocket_result = await _msg.render_and_publish_template(
                user_id=owner_user_id,
                category=category,
                key=key,
                values=values_dict,
                e_tags=e_tags,
                p_tags=p_tags,
                private_key=private_key,
                return_websocket_message=True,
                reply_to_30311_event=reply_to_30311_event,
                reply_to_30311_a_tag=reply_to_30311_a_tag,
                reply_relay=reply_relay,
            )

            if isinstance(websocket_result, tuple):
                # Expected format: (content, goat_data)
                content_to_broadcast, goat_data = websocket_result
            elif isinstance(websocket_result, dict):
                content_to_broadcast = websocket_result.get("content")
                goat_data = websocket_result.get("goat_data")
            elif websocket_result:
                content_to_broadcast = str(websocket_result)
            if content_to_broadcast is None:
                content_to_broadcast = _format_local_content()
                extras_needed = True
        except Exception as e:
            logger.warning(f"Failed to render websocket message: {e}")
            content_to_broadcast = _format_local_content()
            extras_needed = True

        if extras_needed and content_to_broadcast:
            content_to_broadcast = await _append_membership_extras(
                content_to_broadcast,
                message_type or category,
                values_dict,
                reply_to_30311_event=reply_to_30311_event,
                reply_to_30311_a_tag=reply_to_30311_a_tag,
            )
        try:
            payload = {
                "type": message_type or category,
                "message": content_to_broadcast or "",
                "e_tags": e_tags or [],
                "p_tags": p_tags or [],
                "reply_to_30311_event": reply_to_30311_event,
                "reply_to_30311_a_tag": reply_to_30311_a_tag,
                "reply_relay": reply_relay,
            }
            if goat_data is not None:
                payload["goat_data"] = goat_data

            res = await _msg.send_to_websocket_clients(websocket_topic, payload)
            broadcasted = bool(res)
            if broadcasted:
                logger.info(f"cyberherd: message broadcasted to websocket clients on topic={websocket_topic}")
            else:
                logger.warning(f"cyberherd: failed to broadcast message to websocket clients on topic={websocket_topic}")
        except Exception as e:
            logger.error(f"cyberherd: websocket broadcast failed: {e}")
    # Treat broadcast-only as success when nostr is disabled
    return bool(ok or (not nostr_enabled and broadcasted))


async def try_publish_note(
    content: str,
    *,
    e_tags: list[str] | None = None,
    p_tags: list[str] | None = None,
    private_key: str | None = None,
    websocket_topic: str = "cyberherd",
    reply_to_30311_event: str | None = None,
    reply_to_30311_a_tag: str | None = None,
    mirror_to_websocket: bool = True,
    reply_relay: str | None = None,
) -> bool:
    """Publish a note using the shared messaging extension services.
    
    Args:
        content: The note content to publish
        e_tags: Event tags for threading
        p_tags: Pubkey tags
        private_key: Nostr private key for signing
        websocket_topic: WebSocket topic/wallet ID to broadcast to (default: "cyberherd")
        reply_to_30311_event: Optional event ID of the kind 30311 note being replied to
        reply_to_30311_a_tag: Optional `a` tag address for the kind 30311 note
    """
    if not _messaging_available or _msg is None:
        logger.debug("Messaging not available, skipping note publication")
        return False
        
    try:
        logger.info(
            f"cyberherd: try_publish_note content_len={len(content or '')} e_tags={e_tags} p_tags={p_tags}"
        )
    except Exception:
        pass
    try:
        nostr_enabled = True
        try:
            nostr_enabled = await _msg.is_nostr_publishing_enabled()
        except Exception:
            nostr_enabled = True

        ok = False
        if nostr_enabled:
            logger.info(f"cyberherd: publishing note via cyberherd_messaging extension")
            logger.debug(f"cyberherd: publishing content='{content[:100]}...' e_tags={e_tags} p_tags={p_tags}")
            ok = await _msg.publish_note(
                content,
                e_tags=e_tags,
                p_tags=p_tags,
                private_key_hex=private_key,
                reply_to_30311_event=reply_to_30311_event,
                reply_to_30311_a_tag=reply_to_30311_a_tag,
                reply_relay=reply_relay,
            )
            if ok:
                logger.info(f"cyberherd: note published successfully")
            else:
                logger.error(f"cyberherd: note failed to publish - check cyberherd_messaging extension logs for details")
        else:
            logger.info(f"cyberherd: nostr publishing disabled, skipping note publication")
        # Always mirror to websocket for overlays (even when nostr disabled)
        broadcasted = False
        if mirror_to_websocket:
            try:
                res = await _msg.send_to_websocket_clients(
                    websocket_topic,
                    {
                        "type": "nostr_message",
                        "message": content,
                        "e_tags": e_tags or [],
                        "p_tags": p_tags or [],
                        "reply_to_30311_event": reply_to_30311_event,
                        "reply_to_30311_a_tag": reply_to_30311_a_tag,
                        "reply_relay": reply_relay,
                    },
                )
                broadcasted = bool(res)
                if broadcasted:
                    logger.info(f"cyberherd: note broadcasted to websocket clients on topic={websocket_topic}")
                else:
                    logger.warning(f"cyberherd: failed to broadcast note to websocket clients on topic={websocket_topic}")
            except Exception as e:
                logger.error(f"cyberherd: websocket broadcast failed: {e}")
        return ok or (not nostr_enabled and broadcasted)
    except Exception as e:
        logger.warning(f"publish_note failed: {e}")
        return False


# Basic messaging module placeholder
class MessagingModule:
    """Basic messaging module for cyberherd."""
    pass

messaging = MessagingModule()


_TEMPLATE_CACHE: Dict[Tuple[str, str], Tuple[float, List]] = {}
_TEMPLATE_CACHE_TTL = 60.0  # seconds

# Optional enforcement mode for tests: ensures make_messages() was called
# for the same (owner_user_id, event_type) shortly before publish_event_message()
# This helps catch mismatches where callers generate content for one template
# but publish a different one. Disabled by default in production.
_ENFORCE_TEMPLATE_MATCH = False
# Store recent make_messages calls: key=(owner_user_id,event_type) -> (content, timestamp)
_LAST_MAKE_CALLS: dict[tuple[Optional[str], str], tuple[str, float]] = {}


def enable_template_match_assertions() -> None:
    global _ENFORCE_TEMPLATE_MATCH
    _ENFORCE_TEMPLATE_MATCH = True


def disable_template_match_assertions() -> None:
    global _ENFORCE_TEMPLATE_MATCH
    _ENFORCE_TEMPLATE_MATCH = False


async def _get_cached_templates(user_id: str, category: str):
    key = (user_id, category)
    now = time.time()
    cached = _TEMPLATE_CACHE.get(key)
    if cached and (now - cached[0]) < _TEMPLATE_CACHE_TTL:
        return cached[1]
    if not _messaging_available or _msg_crud is None:
        return []
    try:
        templates = await _msg_crud.get_message_templates(user_id, category)
        _TEMPLATE_CACHE[key] = (now, templates)
        return templates
    except Exception:
        return []


async def _get_random_template(user_id: str, category: str):
    """Fetch a random template directly from the cyberherd_messaging extension.
    
    This directly calls the CRUD functions since both extensions run in the same
    process, avoiding unnecessary HTTP overhead.
    """
    if not _messaging_available or _msg_crud is None:
        return None
    
    try:
        # Directly fetch templates from the same process
        templates = await _msg_crud.get_message_templates(user_id, category)
        if templates:
            # Pick a random template
            return random.choice(templates)
        return None
    except Exception as e:
        logger.warning(f"Failed to fetch random template: {e}")
        return None


def _invalidate_template_cache(user_id: str, category: Optional[str] = None):
    if category:
        _TEMPLATE_CACHE.pop((user_id, category), None)
    else:
        # purge all for user
        for k in list(_TEMPLATE_CACHE.keys()):
            if k[0] == user_id:
                _TEMPLATE_CACHE.pop(k, None)


async def publish_event_message(
    event_type: str,
    *,
    owner_user_id: Optional[str] = None,
    values: Optional[dict] = None,
    e_tags: Optional[list[str]] = None,
    p_tags: Optional[list[str]] = None,
    private_key: Optional[str] = None,
    websocket_topic: str = "cyberherd",
    reply_to_30311_event: str | None = None,
    reply_to_30311_a_tag: str | None = None,
    reply_relay: str | None = None,
) -> bool:
    """Unified publisher: choose random template key (if multiple), render and publish.

    Falls back to make_messages() local content when shared template not found.
    Always mirrors to websocket via publish_shared_template logic.
    
    Args:
        event_type: Type of event to publish
        owner_user_id: User ID whose templates to use
        values: Template values
        e_tags: Event tags for threading
        p_tags: Pubkey tags
        private_key: Nostr private key for signing
        websocket_topic: WebSocket topic/wallet ID to broadcast to (default: "cyberherd")
        reply_to_30311_event: Optional event ID for kind 30311 threading
        reply_to_30311_a_tag: Optional `a` tag address for kind 30311 threading
    """
    values = values or {}

    mapping = {
        "headbutt_success": "headbutt_success",
        "headbutt_failure": "headbutt_failure",
        "headbutt_info": "headbutt_info",
        "cyber_herd": "cyber_herd",
        "new_member": "cyber_herd",
        "member_increase": "member_increase",
        # Repost / reaction categories map to their own template categories in the messaging extension
        "kind_6_repost": "kind_6_repost",
        "kind_7_reaction": "kind_7_reaction",
        "zapper_displaces_kind_6": "zapper_displaces_kind_6",
        "zapper_displaces_kind_7": "zapper_displaces_kind_7",
        "kind_6_headbutt_failure": "kind_6_headbutt_failure",
        "kind_7_headbutt_failure": "kind_7_headbutt_failure",
        "sats_received": "sats_received",
        "feeder_triggered": "feeder_trigger",
        "feeder_trigger_bolt12": "feeder_trigger",
        "cyber_herd_treats": "cyber_herd_treats",
        # cyber_herd_info has no matching shared category; let fallback handle it
        "interface_info": "interface_info",
        "variations": "variations",
    }
    
    category = mapping.get(event_type)
    logger.debug(f"publish_event_message: event_type={event_type}, category={category}, owner={owner_user_id}")
    if not category:
        # Unknown -> try local make_messages only
        try:
            msg = await make_messages(event_type, owner_user_id=owner_user_id, **values)
            content = msg.get("content") if isinstance(msg, dict) else str(values)
            payload = msg.get("payload") if isinstance(msg, dict) else None
            content_str = str(content or "")
            content_str = await _append_membership_extras(
                content_str,
                event_type,
                values or {},
                reply_to_30311_event=reply_to_30311_event,
                reply_to_30311_a_tag=reply_to_30311_a_tag,
            )
            
            # Publish to nostr
            nostr_ok = await try_publish_note(
                content_str,
                e_tags=e_tags,
                p_tags=p_tags,
                private_key=private_key,
                websocket_topic=websocket_topic,
                reply_to_30311_event=reply_to_30311_event,
                reply_to_30311_a_tag=reply_to_30311_a_tag,
                mirror_to_websocket=False,
                reply_relay=reply_relay,
            )
            
            # Explicitly send websocket message with semantic event_type
            broadcasted = False
            if _msg is not None:
                try:
                    websocket_message = content_str
                    if isinstance(payload, dict):
                        websocket_message = str(payload.get("message") or websocket_message)
                    message_obj = {
                        "type": event_type,  # Use semantic event_type for websocket
                        "message": websocket_message,
                        "e_tags": e_tags or [],
                        "p_tags": p_tags or [],
                        "values": values,
                        "reply_to_30311_event": reply_to_30311_event,
                        "reply_to_30311_a_tag": reply_to_30311_a_tag,
                        "reply_relay": reply_relay,
                    }
                    if isinstance(payload, dict):
                        for extra_key in ("goat_data", "spots_info", "headbutt_text"):
                            if payload.get(extra_key) is not None:
                                message_obj[extra_key] = payload[extra_key]
                    res = await _msg.send_to_websocket_clients(
                        websocket_topic,
                        message_obj,
                    )
                    broadcasted = bool(res)
                    if broadcasted:
                        logger.info(f"cyberherd: unknown event type message broadcasted to websocket (type={event_type})")
                except Exception as e:
                    logger.warning(f"cyberherd: unknown event type websocket broadcast failed: {e}")
            
            return nostr_ok or broadcasted
        except Exception:
            return False

    # If owner specified, use the new random template API
    if owner_user_id:
        try:
            # Use the new efficient random template API endpoint
            picked = await _get_random_template(owner_user_id, category)
            logger.debug(f"publish_event_message: picked_template={getattr(picked, 'key', None)} for category={category}")
            if picked:
                ok = await publish_shared_template(
                    owner_user_id=owner_user_id,
                    category=category,  # Use category for template lookup
                    key=str(picked.key),
                    values=values,
                    e_tags=e_tags,
                    p_tags=p_tags,
                    private_key=private_key,
                    websocket_topic=websocket_topic,
                    message_type=event_type,  # Use semantic event_type for websocket
                    reply_to_30311_event=reply_to_30311_event,
                    reply_to_30311_a_tag=reply_to_30311_a_tag,
                    reply_relay=reply_relay,
                )
                if ok:
                    return True
        except Exception as e:
            logger.warning(f"Failed to fetch/publish random template: {e}")

    # Fallback local generation
    try:
        msg = await make_messages(event_type, owner_user_id=owner_user_id, **values)
        content = msg.get("content") if isinstance(msg, dict) else str(values)
        payload = msg.get("payload") if isinstance(msg, dict) else None
        
        # Publish to nostr using try_publish_note
        logger.debug(f"publish_event_message: fallback publishing content='{content[:120] if isinstance(content, str) else str(content)[:120]}' e_tags={e_tags} p_tags={p_tags}")
        content_str = str(content or "")
        content_str = await _append_membership_extras(
            content_str,
            event_type,
            values or {},
            reply_to_30311_event=reply_to_30311_event,
            reply_to_30311_a_tag=reply_to_30311_a_tag,
        )
        nostr_ok = await try_publish_note(
            content_str,
            e_tags=e_tags,
            p_tags=p_tags,
            private_key=private_key,
            websocket_topic=websocket_topic,
            reply_to_30311_event=reply_to_30311_event,
            reply_to_30311_a_tag=reply_to_30311_a_tag,
            mirror_to_websocket=False,
            reply_relay=reply_relay,
        )
        
        # Explicitly send websocket message with semantic event_type
        # (instead of relying on try_publish_note's generic "nostr_message" type)
        broadcasted = False
        if _msg is not None:
            try:
                websocket_message = content_str
                if isinstance(payload, dict):
                    websocket_message = str(payload.get("message") or websocket_message)
                message_obj = {
                    "type": event_type,  # Use semantic event_type for websocket
                    "message": websocket_message,
                    "e_tags": e_tags or [],
                    "p_tags": p_tags or [],
                    "values": values,  # Include values for consistency
                    "reply_to_30311_event": reply_to_30311_event,
                    "reply_to_30311_a_tag": reply_to_30311_a_tag,
                    "reply_relay": reply_relay,
                }
                if isinstance(payload, dict):
                    for extra_key in ("goat_data", "spots_info", "headbutt_text"):
                        if payload.get(extra_key) is not None:
                            message_obj[extra_key] = payload[extra_key]
                res = await _msg.send_to_websocket_clients(
                    websocket_topic,
                    message_obj,
                )
                broadcasted = bool(res)
                if broadcasted:
                    logger.info(f"cyberherd: fallback message broadcasted to websocket clients (type={event_type})")
            except Exception as e:
                logger.warning(f"cyberherd: fallback websocket broadcast failed: {e}")
        
        logger.debug(f"publish_event_message: nostr_ok={nostr_ok}, broadcasted={broadcasted} -> returning {nostr_ok or broadcasted}")
        return nostr_ok or broadcasted
    except Exception:
        return False


__all__ = [
    "make_messages",
    "publish_shared_template",
    "publish_event_message",
    "send_cyberherd_update",
    "send_to_websocket_clients",
    "try_publish_note",
]
