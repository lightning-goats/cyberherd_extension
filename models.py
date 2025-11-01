from typing import Optional
from datetime import datetime, timezone

from loguru import logger

from pydantic import BaseModel, Field


class CyberherdSettings(BaseModel):
    source_wallet: Optional[str] = None
    zap_wallet: Optional[str] = None
    zap_wallet_alias: Optional[str] = None
    herd_wallet: Optional[str] = None
    max_members: int = 3
    tracked_tags: list[str] = Field(default_factory=lambda: ["#CyberHerd"])
    tracked_event_ids: list[str] = Field(default_factory=list)  # all tracked event IDs (manual + automatic)
    tracked_event_timestamps: dict[str, int] = Field(default_factory=dict)  # note_id -> created_at timestamp
    tracked_event_addresses: dict[str, str] = Field(default_factory=dict)  # note_id -> a-tag (for kind 30311 replies)
    # hex-encoded nostr private key (32 bytes hex) used to publish notes
    nostr_private_key: Optional[str] = None
    # optional hex-encoded public key override to watch for #CyberHerd notes
    nostr_pubkey_override: Optional[str] = None
    # computed effective pubkey (cached for performance)
    computed_effective_pubkey: Optional[str] = None
    # toggle to enable/disable zap monitoring
    zap_tracking_enabled: bool = True
    # mode for zap monitoring: 'payment' (default) or 'nostr'
    zap_monitor_mode: str = "websocket"
    # owner whose templates in cyberherd_messaging should be used for rendering
    templates_owner_user: Optional[str] = None
    # toggle to opt-in to nightly midnight reset behavior (deactivate members + reset splits)
    midnight_reset_enabled: bool = True
    # toggle to enable/disable repost (kind 6) event tracking
    repost_tracking_enabled: bool = False
    # toggle to enable/disable likes/reactions (kind 7) event tracking
    likes_tracking_enabled: bool = False
    # minimum sats required for zap to count towards cyberherd membership
    minimum_sats: int = 10
    # optional feeder trigger threshold (sats) supplied by external automation
    feeder_trigger_sats: Optional[int] = None
    # toggle to enable/disable NIP-05 verification requirement
    nip05_verification_enabled: bool = True
    # user ID associated with these settings
    user_id: Optional[str] = None


class LegacyCyberherdMember(BaseModel):
    pubkey: str
    alias: Optional[str] = None
    added_at: Optional[int] = None


class CyberHerd(BaseModel):
    pubkey: str
    display_name: Optional[str] = None
    nprofile: Optional[str] = None
    lud16: Optional[str] = None
    notified: Optional[int] = 0
    payouts: float = 0.0
    amount: int = 0
    picture: Optional[str] = None
    metadata_last_checked_at: Optional[int] = None
    is_active: int = 0
    event_id: Optional[str] = None
    note: Optional[str] = None
    kinds: Optional[str] = None
    relays: Optional[str] = None


class MemberPut(BaseModel):
    """Payload model for updating/creating members via API.

    Fields mirror what the views expect: optional amount (sats), an optional
    zap_request dict (as provided by middleware), optional note/event id,
    and optional relays/reply_relay hints used for messaging.
    """
    pubkey: Optional[str] = None
    amount: Optional[int] = None
    zap_request: Optional[dict] = None
    note: Optional[str] = None
    relays: Optional[object] = None
    reply_relay: Optional[str] = None


# CyberherdMember model for database operations
class CyberherdMember:
    """Model representing a CyberHerd member."""

    def __init__(self, pubkey: str, amount: int, allocation_percentage: float = 10.0, note_id: str = None, created_at: datetime = None):
        self.pubkey = pubkey
        self.amount = amount
        self.allocation_percentage = allocation_percentage
        self.note_id = note_id
        self.created_at = created_at or datetime.now(timezone.utc)
        self.is_active = True
        self.display_name = None
        self.lud16 = None
        self.picture = None
        self.relays = None
        self.metadata_last_checked_at = None
        self.payouts = 0.0
