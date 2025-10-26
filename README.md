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

## Development Notes

- Runtime bootstrapping creates or upgrades required tables (`cyber_herd`, `processed_events`, `processed_zaps`, `zap_totals`) on demand.
- The zap monitor only trusts invoice settlements; Nostr zap events (kind 9734/9735) are intentionally ignored to avoid duplicates.
- Metadata caching can be tuned with the environment variables:
  - `CYBERHERD_METADATA_REFRESH_SECONDS` (default 3600 seconds)
  - `CYBERHERD_NIP05_REFRESH_SECONDS` (default 10800 seconds)
- To debug subscriptions, set `CYBERHERD_DEBUG=true` before launching LNbits.

## Contributing

Pull requests are welcome! Please ensure that:

- Linting/tests pass locally.
- Headbutt admission logic stays aligned with middleware behaviour.
- README and docs remain updated when behaviour changes.

Licensed under the MIT License.
