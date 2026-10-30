# Joining the CyberHerd: How It Works

## What is the CyberHerd?

The #CyberHerd is a community of Nostr users who collectively support the ⚡Lightning Goats⚡ project. As a herd member, you'll receive a share of all payments sent to feed the goats every time the feeder is triggered!

**Important: With midnight reset enabled, the CyberHerd resets every night at midnight in the server's local timezone.** All memberships and accumulated zap amounts are cleared, giving everyone a fresh start each day. This means:
- All members are removed from the herd
- All accumulated zap amounts and first-repost/first-like awards reset to zero
- A current-day #CyberHerd note from the project is required; posting that note is handled by the project's separate publishing process

## How to Join the CyberHerd

The CyberHerd operates continuously with automatic monitoring of Nostr interactions:

1. **Find TODAY'S CyberHerd note**: Look for the current day's note tagged with #CyberHerd from the Lightning Goats project
2. **Zap the note**: Send a zap meeting the configured minimum to join the herd and earn payouts (10 sats in the examples below)
3. **Payment confirmation**: A zap is processed after the invoice settles and its signed request and project note are verified
4. **Notification**: You'll be notified via public reply when you join the herd

**Requirements for new membership:**
- Valid NIP-05 identifier (your verified Nostr address) - can be disabled in settings
- Working Lightning address to receive payments (required for repost/reaction-only members)
- Zap TODAY'S #CyberHerd tagged note with minimum sats (configurable, typically 10+ sats)
- Alternative: Repost (kind 6) or React (kind 7) to TODAY'S #CyberHerd note if herd has open slots

**Note**: Only zaps to Lightning Goats project notes are processed for CyberHerd membership. Due to the daily reset, you must use the current day's #CyberHerd note to join.

## Reposts and Reactions: Free Entry with Limitations

Members can join the CyberHerd without zapping by reposting or reacting to TODAY'S #CyberHerd note:

### Repost (Kind 6) and Reaction (Kind 7) Rules:
- **Free entry when slots available**: If the herd has open slots, reposts and reactions can join without paying
- **TODAY'S note requirement**: Must repost or react to the current day's #CyberHerd note (older notes invalid)
- **Separate cumulative rewards**: The first repost earns 3 points and the first like earns 1 point. Doing both earns 4 points; there is no fixed percentage bonus.
- **Zaps are the strongest incentive**: A zap meeting the configured joining minimum earns 10 points. Every additional settled zap adds points in proportion to its satoshi amount. Social points add to zap points.
- **Valid requirements**: NIP-05 identifier (configurable, can be disabled) and Lightning address required
- **Vulnerable to displacement**: A new or rejoining zap that meets the configured minimum will displace repost/reaction-only members first
- **Repost headbutting (kind 6)**: Reposts CAN headbutt other repost-only or reaction-only members (amount=0) when the herd is full
- **Reaction no headbutting (kind 7)**: Reactions CANNOT headbutt anyone - can only join when slots are available
- **Can upgrade to zapper**: Existing repost/reaction members can zap ANY Lightning Goats note to increase their amount and secure a larger payout share

### Priority Hierarchy:
When the herd is full:
1. **Zaps**: Displace repost/reaction members first (amount = 0 sats), then compete with other zappers based on amount
2. **Reposts (kind 6)**: Can displace other repost-only or reaction-only members (amount = 0 sats)
3. **Reactions (kind 7)**: Cannot headbutt - must wait for open slots or can zap or repost to increase headbutting abilities.

### Why Join via Repost/Reaction:
- **Risk-free participation**: Join without financial commitment
- **Learn the system**: Experience CyberHerd mechanics before investing
- **Upgrade later**: Zap any Lightning Goats content to build your amount and secure your position
- **Support visibility**: Reposts amplify Lightning Goats content even if you receive no payout

### Example Progression:
- **Day 1**: Repost TODAY'S #CyberHerd note → Join with 0 sats → Earn 3 points and a proportional share of the member pool
- **Day 2**: Zap 50 sats to TODAY'S #CyberHerd note → Join with 50 sats → Start receiving proportional payouts
- **Later on Day 2**: Already a member, zap 100 sats to any Lightning Goats post → Total becomes 150 sats → Harder to displace

## Increasing Your Payouts (Existing Members)

Once you're a CyberHerd member, you have **flexible zapping options** to increase your payouts:

### For New Members (Joining):
- **Must zap TODAY'S #CyberHerd note**: New members can only join by zapping the current day's note tagged with #CyberHerd
- **Daily note validity**: Only the #CyberHerd note posted for the current day is valid for joining (older notes don't work due to daily reset)
- **Structured onboarding**: This ensures proper introduction to the CyberHerd system for each daily cycle

### For Existing Members (Increasing payouts):
- **Zap ANY Lightning Goats note**: You can zap any note authored by the Lightning Goats project to increase your payout share
- **No #CyberHerd tag required**: Support any Lightning Goats content - technical posts, goat updates, educational content, etc.
- **Cumulative increases**: Every settled qualifying zap increases your total contribution and payout share; duplicate delivery of the same payment counts once
- **Threaded responses**: Thank you messages will appear as replies to whatever note you zapped

### How It Works:
- **Payment detection**: Only zaps to Lightning Goats authored notes are detected by the payment system
- **Member status check**: System automatically checks if you're already a CyberHerd member
- **Flexible support**: Existing members can engage with the full Lightning Goats content ecosystem
- **Content discovery**: Encourages exploration and support of diverse Lightning Goats topics

## How Payouts Work

**Daily Reset Notice**: When midnight reset is enabled, membership and its daily contribution scores start fresh at midnight in the server's configured local timezone. If the service misses midnight, it catches up before processing new admissions or distributions after restart. Lifetime zap history and replay-protection records are retained.

After the feeder integration successfully requests a feed at its configured threshold, the available herd balance is distributed:

- **Contribution points** - A minimum qualifying zap earns 10 points, the first repost earns 3, and the first like earns 1. These rewards add together.
- **One shared pool** - Eligible active members divide the configured member allocation proportionally to their total points.
- **Every paid invoice counts once** - Re-delivery does not add points; a distinct settled invoice does.
- **Only Lightning Goats content** - Zaps to other people's notes don't count
- **Feeder-triggered payments** - Reaching a balance threshold alone does not distribute funds. A blocked, failed, or unknown feeder request does not authorize a payout
- **Bigger contributions = bigger share** - The more you've zapped, the larger your piece of the available allocation
- **Your zaps add up over time** - Every qualifying zap increases your total and future payouts
- **Direct to your Lightning address** - No need to claim anything, payments are automatic
- **Configurable maximum members** - Limited spots make membership valuable
- **Configurable member allocation** - Examples below use 10% for members and the remainder for goats; the operator may configure a different percentage
- **Existing members have flexibility** - Once you're in, you can zap any Lightning Goats content to increase payouts

## How Payments Work

After a successful feeder request, the herd balance is split using the current members and configured allocation:

- **Goat allocation** - The remainder after payable member shares goes to goat food and operations (90% in these examples)
- **Member allocation** - A configurable percentage (10% in these examples) is divided by cumulative contribution points

### Your Share
The more you've zapped Lightning Goats content, the bigger your share of that 10%! Your payment gets sent directly to your Lightning address when the feeder triggers.

### Simple Example:
- Feeder triggers with 1,000 sats
- 900 sats feeds the goats
- 100 sats gets split among CyberHerd members
- The member with the most contribution points gets the biggest piece!

## Payout Examples

Here's how your share gets calculated:

**Example 1 - Simple scenario:**
- You've zapped 500 sats total to Lightning Goats content
- Other members zapped 300 sats and 200 sats each
- When the feeder triggers with 1,000 sats, you get the biggest share of the 100 sat CyberHerd payout!

**Example 2 - Building your share (existing members):**
- You start by zapping 200 sats to a #CyberHerd note to join
- Later you zap 300 sats to a Lightning Goats educational post (because you're already a member!)
- Then 100 sats to a goat update photo
- Your total: 600 sats = bigger share of future payouts!

**Example 3 - Real payout:**
- Feeder triggers with 5,000 sats
- Your share: ~150 sats sent directly to your Lightning address (based on your contribution ratio)

**Point formula**

`points = 10 × qualifying zap sats ÷ max(1, configured joining minimum) + 3 × first repost + 1 × first like`

At a 10-sat joining minimum, every sat earns one point. If the minimum changes,
zap points scale with it so a minimum qualifying zap always earns 10 points.
A zero configured minimum uses a one-sat basis for scoring. Only positive,
settled zaps count. Every distinct qualifying paid invoice adds its points.

`member percentage = member allocation × member points ÷ total eligible active member points`

The configured goat allocation remains outside the member pool. An eligible
member gets no fixed percentage; their share changes as others earn points.
Only active members with a valid Lightning address participate in the pool.
If nobody is eligible, the configured goat wallet receives the full payment.

**Example 4 - Zaps, reposts and likes ranked separately:**
- Joining minimum: 10 sats; member pool: 10% of the full payment
- Zap-only member: 10 sats → 10 points
- Repost-only member: 3 points; like-only member: 1 point
- Total: 14 points; full-payment shares: approximately 7.142857%, 2.142857%, and 0.714286%
- The goat wallet receives 90%

**Example 5 - Cumulative engagement:**
- Member A zaps 20 sats, reposts and likes: 20 + 3 + 1 = 24 points
- Member B zaps 20 sats without social engagement: 20 points
- Their shares of a 10% member pool are approximately 5.454545% and 4.545455%
- Reposting or liking again adds no points. A further distinct 5-sat zap raises A to 29 points

**Once-per-cycle social awards:**
- The first successfully admitted repost and first successfully admitted like each count once per project owner and Nostr pubkey across all qualifying notes
- Repeating the interaction, using a new event ID, restarting the service, or unliking/reposting again does not renew an award
- Eviction or deleting a member does not erase their earned social awards or allow the same type to grant free re-entry. A previously unused social type can still qualify under the normal admission rules
- Returning with a qualifying zap restores the previously earned social points; the eviction-reset zap score starts from the new contribution
- Enabled midnight reset clears social awards with daily membership scores. With midnight reset disabled, awards persist until a manual reset
- Manual reset starts a fresh cycle and clears the awards atomically with the membership state
- Failed admissions earn no social points. Replaying an old event after a daily reset does not grant a fresh award

## The ⚡Headbutting⚡ Mechanism

When the CyberHerd reaches maximum capacity, new members can still join by "headbutting" existing members:

### How Headbutting Works:
- **Zap requirement**: Only zaps of 10+ sats can trigger headbutting
- **Target selection**: You must exceed the lowest contributing member's total zap amount
- **Automatic process**: If your zap qualifies, the system automatically removes the lowest member
- **Instant replacement**: You immediately take their spot in the herd

### Headbutting Examples:

**Scenario 1 - Basic headbutt (new member joining):**
- Current herd: members with zap totals of [1000, 500, 200]
- You zap: 250 sats to TODAY'S #CyberHerd note → You replace the 200-sat member
- New herd: [1000, 500, 250] (you're now in the herd)
- The headbutted member's 200 sats are reset to zero

**Scenario 2 - Failed headbutt:**
- Current herd: members with zap totals of [1000, 500, 200 sats]  
- You zap: 150 sats to TODAY'S #CyberHerd note → Headbutt fails (need 201+ sats to exceed 200)
- You get notified of the failure

**Scenario 3 - Existing member zaps more (flexible content):**
- You're in the herd with 300 sats total
- You zap 200 sats to ANY Lightning Goats note → Your total becomes 500 sats
- Your payout share increases, harder to get headbutted out

**Scenario 4 - Getting headbutted and rejoining:**
- You're in the herd with 400 sats total
- Someone headbutts you out with 450 sats to TODAY'S #CyberHerd note → You lose your spot AND your previous zap amount is reset to zero
- To rejoin: You must start fresh with a new zap to TODAY'S #CyberHerd note that exceeds the current lowest member
- Your accumulated amount from before getting headbutted is completely reset

**Scenario 5 - Repost/reaction member displaced by zap:**
- Current herd: [Repost (0 sats), Zap (100 sats), Zap (200 sats)]
- You zap: 10 sats to TODAY'S #CyberHerd note → You replace the repost member
- New herd: [Zap (10 sats), Zap (100 sats), Zap (200 sats)]
- Zaps meeting the configured minimum displace repost/reaction members first; this example assumes a 10-sat minimum

**Scenario 6 - Repost/reaction joining open slots:**
- Current herd: [Zap (500 sats), empty, empty]
- You repost TODAY'S #CyberHerd note → You join without headbutting
- New herd: [Zap (500 sats), Repost (0 sats), empty]
- Reposts and reactions can join when slots are available without paying

**Scenario 7 - Repost headbutts reaction member:**
- Current herd: [Reaction (0 sats), Zap (100 sats), Zap (200 sats)]
- You repost TODAY'S #CyberHerd note → You replace the reaction member
- New herd: [Repost (0 sats), Zap (100 sats), Zap (200 sats)]
- Reposts can headbutt repost-only or reaction-only members when the herd is full

**Scenario 8 - Repost headbutts another repost member:**
- Current herd: [Repost (0 sats), Zap (100 sats), Zap (200 sats)]
- You repost TODAY'S #CyberHerd note → You replace the existing repost member
- New herd: [Repost (0 sats - you), Zap (100 sats), Zap (200 sats)]
- Reposts can replace other repost members (most recent repost wins)

**Scenario 9 - Reaction cannot headbutt:**
- Current herd: [Repost (0 sats), Zap (100 sats), Zap (200 sats)]
- You react to TODAY'S #CyberHerd note → Headbutt fails (reactions cannot headbutt)
- Herd unchanged: [Repost (0 sats), Zap (100 sats), Zap (200 sats)]
- Reactions can only join when there are open slots available

### Protection Strategy:
- **Build your total**: Multiple zaps accumulate, making you harder to headbutt
- **Monitor the herd**: Check current members' contributions before zapping
- **Stay active**: Regular zapping increases your "headbutt resistance"
- **Risk awareness**: Getting headbutted resets your membership zap score; prior donations are not refunded or carried over when you rejoin. Earned social awards remain recorded until the cycle resets

### Headbutting Rules:
- **New members only**: Only new members can attempt headbutting by zapping TODAY'S #CyberHerd note or reposting it
- **Daily note requirement**: Must use the current day's #CyberHerd note (older notes invalid due to daily reset)
- **Existing member flexibility**: Current members can increase payouts by zapping ANY Lightning Goats note with any positive amount
- **Minimum zap for new or rejoining members**: The configured minimum applies even when displacing a free member. Existing active members may add any positive amount
- **Repost headbutting (kind 6)**: Reposts can join and headbutt repost-only or reaction-only members when herd is full
- **Reaction bypass (kind 7)**: Reactions can only join when herd has open slots (cannot headbutt)
- **Zap priority**: Zaps always displace repost/reaction members first before competing with other zaps
- **Repost targets**: Reposts can only displace members with amount=0 (other reposts or reactions)
- **Amount reset**: When headbutted out, your accumulated amount is completely reset to zero
- **Fresh start**: To rejoin after being headbutted, you start with zero sats and must zap or repost TODAY'S #CyberHerd note again
- **Public notifications**: Both parties receive public reply notifications about headbutts
- **Instant processing**: Headbutt attempts are processed immediately when the herd is full

## System Features

The CyberHerd works automatically behind the scenes:

- **Always watching**: Continuously monitors for zaps to Lightning Goats content
- **Confirmed updates**: Settled qualifying zaps and authenticated reposts/reactions update membership; pending invoices do not count
- **Replay protection**: Repeated delivery of the same payment or social event counts once, including after a restart
- **Public notifications**: You'll get public replies when you join, get headbutted, or other important events happen

## Getting Started

1. **Set up your profile**: Ensure you have a valid NIP-05 identifier and working Lightning address configured
2. **Find TODAY'S CyberHerd note**: Look for the current day's #CyberHerd tagged note by the Lightning Goats project (older #CyberHerd notes won't work due to daily reset)
3. **Choose your entry method**:
   - **Zap to join**: Send 10+ sats to join with payout eligibility
   - **Repost to join**: Free entry with 0 sats, can headbutt other repost/reaction members when herd is full
   - **React to join**: Free entry with 0 sats, can only join when slots are available (cannot headbutt)
4. **Handle full herd**: If the herd is full (3 members):
   - **Zaps**: Will displace repost/reaction members first, then compete with other zappers by amount
   - **Reposts (kind 6)**: Can headbutt other repost-only or reaction-only members (amount=0)
   - **Reactions (kind 7)**: Cannot join when herd is full (cannot headbutt)
5. **Explore flexible zapping**: Once a member, zap ANY Lightning Goats note to increase your payout share and headbutt protection
6. **Monitor the herd**: Check current members to plan your strategy
7. **Stay active**: Regular zaps to Lightning Goats content build up your total contribution and payout share
8. **Daily renewal**: Remember the herd resets at midnight (Local Time) - you'll need to rejoin each day using that day's #CyberHerd note

## Important Notes

- **Daily Reset**: The CyberHerd resets every night at midnight (Local Time) - all memberships and zap amounts are cleared (opt-in, configurable)
- **Today's note only**: Only the current day's #CyberHerd tagged note is valid for joining (older notes don't work)
- **Requirements**: NIP-05 identifier (configurable, can be disabled) and working Lightning address required
- **Content scope**: Only zaps to Lightning Goats authored notes qualify for membership and payouts
- **New members via zap**: Must zap TODAY'S #CyberHerd tagged note to join (minimum sats configurable, typically 10+)
- **New members via repost**: Can repost TODAY'S #CyberHerd note to join with 0 sats (can headbutt other repost/reaction members)
- **New members via reaction**: Can react to TODAY'S #CyberHerd note to join with 0 sats (only when slots available, cannot headbutt)
- **Existing members**: Can zap ANY Lightning Goats note to increase payouts with any positive amount
- **Maximum members**: Configurable (typically 3) - full herd requires headbutting out lowest member to join
- **Zap priority**: Zaps always displace repost/reaction-only members first before competing with other zaps
- **Repost headbutting**: Reposts (kind 6) can displace repost-only or reaction-only members (amount=0)
- **Reaction limitation**: Reactions (kind 7) cannot headbutt - can only join when open slots available
- **Cumulative points**: A minimum qualifying zap earns 10 points; the first repost earns 3 and the first like earns 1. Every distinct qualifying settled zap adds points
- **Proportional shares**: The member allocation is divided according to total eligible active member points
- **Social awards count once**: Each social type counts once per owner/pubkey/cycle and survives eviction or deletion
- **Headbutt consequences**: Getting headbutted resets the membership zap score to zero. Social awards remain recorded and contribute again only after valid re-entry
- **Threshold payout cycles**: Each full threshold cycle transfers exactly the configured trigger amount from the herd wallet to the CyberHerd distribution wallet. The remainder stays in the herd wallet. With a 1,000-sat trigger, 1,234 sats produces one feeding and one 1,000-sat transfer, leaving 234 sats. A 2,345-sat balance produces two feedings and two separate 1,000-sat transfers, leaving 345 sats. Each received payment produces one feeder-triggered announcement for its entire batch. At most three feeder activations are rewarded per received payment. Additional full thresholds still produce separate CyberHerd payouts: 4,345 sats means three feeder activations and four 1,000-sat transfers, leaving 345 sats. Within one payment, feeder requests are spaced six seconds apart; separate payments retain the configured cooldown. Both respect the override switch.
- **CyberHerd treats**: Confirmed member payouts receive a named Nostr mention in that member's thread, replying to their joining message for the membership cycle. Later membership updates reuse that branch. New membership cycles start new branches; late settlements stay attached to their original cycle.
- **Small shares**: A share that cannot cover the routing reserve and a whole-satoshi payment, or the recipient's minimum, is skipped. “The big goats ate all the food this round” explains the missed treat. The `cyber_herd_no_treats` category provides varied goat-themed explanations; confirmed payments use the existing `cyber_herd_treats` category. All wording, including optional repost hints and feeding summaries, comes from editable `cyberherd_messaging` database templates. An occasional repost suggestion appears only if the member has not already earned the repost award, at most once per membership cycle. Skipped amounts are not credited as a member balance or carried forward.
- **Reliable announcements**: `cyberherd_messaging` renders, signs and submits all member-thread messages. Pending or failed payments are never announced as received. Messages survive a restart and reuse the same signed event on retry. Goat operations payouts and ambiguous shared recipient addresses do not produce member treat notices.
- **Existing members after upgrade**: Older joining messages have no stored publication receipt. Their first update creates a clearly labeled member-thread anchor rather than pretending to know the old joining event ID. New joins retain the actual signed event ID.
- **Feeding recovery**: Confirmed feeding requests resume their recorded transfer after interruption. An uncertain feeder request is held for operator reconciliation rather than automatically repeated.
- **Configurable split**: The configured member allocation limits the CyberHerd pool; unused member allocation goes to the goat wallet. There is no one-percentage-point reservation or minimum allocation per slot

Payout examples show amounts before routing fees and whole-satoshi rounding by the payment service. Proportional split targets retain six decimal places of percentage precision. A successful OpenHAB rule request authorizes distribution; physical delivery requires separate feeder telemetry.

## Payment and history rules

- Only settled incoming payments to the configured herd wallet count as zaps. The signed request must identify the sender and reference an authentic project note. Tracking must be enabled.
- New and returning members use today's project-tagged note and satisfy the current entry requirements. An old database record does not bypass those requirements.
- Active members may add any positive zap to any authentic project-authored note; a #CyberHerd tag is not required for those additions.
- Each paid invoice counts once. Separate settled invoices count separately even if they reuse the same signed zap request. Re-delivering the same repost or reaction does not create another headbutt.
- A settled qualifying zap can appear in lifetime history even when it is insufficient to join or displace someone. It does not grant a membership score for a rejected admission. History refresh uses verified settled payments held by this installation; relay receipts alone do not prove payment.
- Payout targets include only members with a valid Lightning address. A missing or malformed address does not receive a target; remaining allocation follows the configured goat-wallet rules. Address syntax validation cannot guarantee that a remote payment provider is available.

Legacy PostgreSQL zap accounting repair (migration 39): some installations still
have a timestamp `processed_zaps.processed_at` column despite the earlier schema
sync. Zap accounting writes text timestamps, so that mismatch aborts the atomic
membership update. Migration 39 converts the column to text in place, preserving
history and the insertion default, and propagates failures so an unsuccessful
repair cannot be recorded as complete. Failed admissions remain eligible for the
existing settled-payment recovery path; accounting still deduplicates by payment.

Migration 40 completes legacy PostgreSQL zap-table compatibility by removing
receipt-specific NOT NULL constraints that the consolidated schema no longer
requires. Payment accounting is identified by the settled payment hash in
`event_id`; the user-scoped unique index continues to prevent double counting.
Recovery diagnostics now distinguish processing attempts from successful zaps.
Background note validation queries the configured author and checks signatures,
tags, and today's time boundaries even when there is no FastAPI app reference.

Database audit changes (migration 41): user-specific reads cannot inherit a
legacy operator's settings. Settings creation preserves the supplied split
switch and zero allocations. Member mutations and their payout recalculations
commit together; payout calculation uses the final engagement kinds. A caught
SQL error still forces the entire admission to roll back, and split updates run
only after commit. Banned members must be unbanned before reactivation.

Manual reset now clears membership scores, event/payment ledgers, cached lifetime
totals, and tracked notes in one transaction. It records pending split work for
retry if the downstream reset fails. The daily rollover retains lifetime history
and replay protection, as described above. Manual amount edits assign the desired
total atomically; failed edits remain retryable and duplicate IDs are user-scoped.
Payment accounting retains the target note for filtered history queries. History
pagination compares parsed timestamps, including legacy ISO values, and reports
whether additional matching rows exist.

Migration 41 repairs event identity indexes and legacy helper columns without
rebuilding PostgreSQL member rows. PostgreSQL monetary totals and epoch seconds
use 64-bit integers; cache update timestamps use double precision. Historical
migration failures propagate instead of being recorded as successes, SQLite
table swaps are transactional, and migrations run in numerical definition order.

Contribution points upgrade (migration 42): existing membership kinds and recorded
successful interactions from the current cycle seed the social-award ledger.
Failed interactions do not seed awards, and unknown award times are left unknown.
No old zap history is converted into a new daily membership score. At startup,
stored payouts and configured split targets are refreshed before event intake.

Displayed percentages use the shared six-decimal calculation. If the payout
backend's storage rounding would push the target total above 100%, target values
are rounded downward only as needed to fit its numeric precision. The tiny
residual stays in the source wallet; member priority and contribution points do
not change. Routing fees and whole-satoshi rounding still apply to payments.
