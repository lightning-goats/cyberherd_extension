# CyberHerd LNbits Extension

CyberHerd is an LNbits extension that automates daily herd management for the Lightning Goats project.  
It watches Lightning Network zaps and Nostr activity, keeps the herd roster up to date, and publishes public updates when members join, get headbutted, or boost their contribution.

## Key Responsibilities

- **Zap-driven admission** – Monitors LNURLp settlements for Lightning Goats wallets and converts qualifying zaps into herd membership updates.
- **Nostr engagement tracking** – Subscribes to relays for repost and reaction events so free-entry slots can be awarded when the herd has openings.
- **Headbutt automation** – Runs the same admission logic as the middleware to determine who gets displaced when the herd is full.
- **Messaging integration** – Uses the optional `cyberherd_messaging` extension to broadcast herd updates to WebSocket clients and publish templated replies on Nostr.
- **Resilient metadata handling** – Caches NIP-05 and lightning address details, retries verification gracefully, and falls back to existing member records during relay hiccups.

## Getting Started

1. Install/enable the extension inside LNbits.
2. Configure your CyberHerd settings:
   - Source and herd wallets
   - Tracked tags (default `#CyberHerd`)
   - Nostr key material or override pubkey
3. Ensure the nostrclient extension is available so the subscription adapter can connect to relays.
4. Restart LNbits to allow the startup task to initialise subscriptions and zap monitors.

The full user-facing walkthrough—including daily reset behaviour, headbutt rules, and payout examples—is available in [cyberherd_explanation.md](./cyberherd_explanation.md).

## API Endpoints

All endpoints are mounted under `/cyberherd/api/v1`. Authentication uses LNbits API keys (admin, invoice, or wallet keys depending on endpoint). Full interactive documentation is available at `/docs#/cyberherd` when the extension is enabled.

### Settings & Configuration

#### `GET /api/v1/settings`

Get current CyberHerd settings for the authenticated user or global defaults.

**Auth:** Optional (wallet/admin key, or unauthenticated for read-only)  
**Returns:** Settings object including tracked tags, wallets, Nostr keys, feature toggles

#### `PUT /api/v1/settings`

#### `POST /api/v1/settings`

Update CyberHerd settings for the authenticated user.

**Auth:** Admin key required  
**Body:** Settings object (tracked_tags, source_wallet, herd_wallet, nostr keys, etc.)  
**Returns:** Updated settings

#### `GET /api/v1/source_wallet`

Get the currently configured source wallet ID.

**Auth:** Wallet or admin key  
**Returns:** `{"source_wallet": "wallet_id"}`

#### `PUT /api/v1/source_wallet`

Update the source wallet ID.

**Auth:** Admin key required  
**Body:** `{"source_wallet": "wallet_id"}`

### Members Management

#### `GET /api/v1/members`

List all herd members with their current status, splits, and tracked note counts.

**Auth:** Wallet or admin key  
**Returns:** Array of member objects with pubkey, display name, status, splits percentage, note counts

#### `POST /api/v1/members`

Manually add a new member to the herd.

**Auth:** Admin key required  
**Body:** Member data (pubkey, display_name, nip05, lightning_address, etc.)  
**Returns:** Created member object

#### `PUT /api/v1/members/{pubkey}`

Update an existing member's details.

**Auth:** Admin key required  
**Path:** `pubkey` - Member's hex pubkey or npub  
**Body:** Updated member fields  
**Returns:** Updated member object

#### `DELETE /api/v1/members/{pubkey}`

Remove a member from the herd.

**Auth:** Admin key required  
**Path:** `pubkey` - Member's hex pubkey or npub

#### `POST /api/v1/members/{pubkey}/activate`

Activate a member (make them eligible for splits).

**Auth:** Admin key required  
**Path:** `pubkey` - Member's hex pubkey or npub

#### `POST /api/v1/members/{pubkey}/deactivate`

Deactivate a member (remove from splits but keep in database).

**Auth:** Admin key required  
**Path:** `pubkey` - Member's hex pubkey or npub

#### `POST /api/v1/members/{pubkey}/ban`

Ban a member (permanently deactivate and mark as banned).

**Auth:** Admin key required  
**Path:** `pubkey` - Member's hex pubkey or npub

#### `POST /api/v1/members/{pubkey}/unban`

Unban a previously banned member.

**Auth:** Admin key required  
**Path:** `pubkey` - Member's hex pubkey or npub

### Monitoring & Diagnostics

#### `GET /api/v1/diagnostics`

Get comprehensive diagnostic information about CyberHerd's runtime state.

**Auth:** Wallet or admin key  
**Returns:** Diagnostics object including:

- Subscription status (relay connections, active subscriptions)
- WebSocket monitor status
- Zap monitor status
- Nostr helpers statistics
- Time boundaries (UTC/local)
- Feature availability flags

#### `GET /api/v1/zap_monitors`

Get status of active zap monitoring instances.

**Auth:** Wallet or admin key  
**Returns:** Map of user IDs to zap monitor states

#### `GET /api/v1/today_notes`

Get IDs of today's notes matching tracked tags, authored by the project account.

**Auth:** Wallet or admin key  
**Query params:**

- `user_id` (optional) - Filter to specific user's tracked tags

**Returns:** `{"note_ids": ["id1", "id2", ...], "count": N}`

#### `GET /api/v1/zap_totals/{zapper_pubkey}`

Get zap totals for a specific zapper (contributor).

**Auth:** Wallet or admin key  
**Path:** `zapper_pubkey` - Hex pubkey of the zapper  
**Returns:** Zap totals object with amounts and counts

#### `POST /api/v1/zap_totals/backfill_payments`

Rebuild zap totals by scanning the herd wallet's LNbits payments (instead of relay data).

**Auth:** Wallet or admin key  
**Body (optional):** `{"zapper_pubkey": "<hex>", "batch_size": 250}`  
**Returns:** Rebuild statistics (`payments_scanned`, `zap_candidates`, `zappers_updated`, etc.)

#### `GET /api/v1/leaderboard`

Public leaderboard data for a CyberHerd user.

**Query params:** `user_id` (optional) or `pubkey` (optional but required if user_id omitted).  
**Returns:** Array of members with `display_name`, `picture`, `amount`, and `is_active` sorted by sats

> Tip: A static leaderboard page is available at `/cyberherd/static/leaderboard/<EffectivePubkey>`.  
> It streams updates over the extension’s own websocket feed (`/cyberherd/ws/leaderboard/<pubkey>`), so no wallet keys or user IDs are exposed.  
> When LNbits is proxied behind a different host/port, you can override the websocket target via `lnbits_host`, `lnbits_port`, or `lnbits_scheme` (e.g., `/cyberherd/static/leaderboard/<pubkey>?lnbits_host=lnbits&lnbits_port=5000&lnbits_scheme=wss`).

### Operations

#### `POST /api/v1/recover_events`

Manually trigger recovery of missed events (notes, reposts, reactions, zaps) from today.

**Auth:** Admin key required  
**Body (optional):** `{"background": true}` - Run in background mode  
**Returns:** Recovery diagnostics including:

- `reposts_reactions`: Count and errors for note/engagement recovery
- `zaps`: Count and errors for payment-based zap recovery
- `messages`: Human-readable status messages

**Background mode:** When `background=true`, returns immediately with a task ID. Check status at `/api/v1/recover_events/status/{task_id}`.

#### `GET /api/v1/recover_events/status/{task_id}`

Check status of a background recovery task.

**Auth:** Wallet or admin key  
**Path:** `task_id` - Task ID from background recovery request  
**Returns:** Task status object (pending, running, completed, failed) with diagnostics

#### `POST /api/v1/manual_reset`

Manually reset the herd: deactivate all members and clear processed zap history.

**Auth:** Admin key required  
**Returns:** Reset confirmation

#### `POST /api/v1/pay_members`

Transfer full balance from herd_wallet to source_wallet (internal wallet-to-wallet payment).

**Auth:** Admin key required  
**Returns:** Payment result with amount transferred

### Authentication

Most endpoints require authentication via LNbits API keys passed in headers:

```bash
# Admin key (full access)
curl -H "X-Api-Key: your_admin_key" https://your-lnbits/cyberherd/api/v1/settings

# Invoice key (read-only for most endpoints)
curl -H "X-Api-Key: your_invoice_key" https://your-lnbits/cyberherd/api/v1/members
```

Interactive API documentation with example requests is available at `/docs#/cyberherd` in your LNbits instance.

## Development Notes

- Runtime bootstrapping creates or upgrades required tables (`cyber_herd`, `processed_events`, `processed_zaps`, `zap_totals`) on demand.
- The zap monitor only trusts invoice settlements; Nostr zap events (kind 9734/9735) are intentionally ignored to avoid duplicates.
- Metadata caching can be tuned with the environment variables:
  - `CYBERHERD_METADATA_REFRESH_SECONDS` (default 3600 seconds)
  - `CYBERHERD_NIP05_REFRESH_SECONDS` (default 10800 seconds)

## Contributing

Pull requests are welcome! Please ensure that:

- Linting/tests pass locally.
- Headbutt admission logic stays aligned with middleware behaviour.
- README and docs remain updated when behaviour changes.

Licensed under the MIT License.
