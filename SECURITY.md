# Security Policy

`cyberherd_extension` is a security-sensitive LNbits extension.

The extension handles interactions involving the Bitcoin Lightning Network and may process payments, LNURL requests, user-supplied messages, Nostr events, public web requests, browser/OBS output, and automation associated with the Lightning Goats project.

A security defect may therefore affect:

* Bitcoin payments;
* LNbits accounts or wallets;
* Lightning payment attribution;
* user privacy;
* displayed or broadcast content;
* connected automation;
* physical feeder activation;
* availability of the LNbits instance.

Security vulnerabilities should be reported privately.

**Do not open a public GitHub issue containing details of an unpatched vulnerability.**

## Supported Versions

Security fixes are primarily provided for the latest maintained release and current development branch.

| Version                   | Supported   |
| ------------------------- | ----------- |
| `main` / latest release   | ✅           |
| Older maintained releases | Best effort |
| Unmaintained versions     | ❌           |
| Third-party forks         | ❌           |

Operators should run the latest stable version whenever practical.

# Reporting a Vulnerability

Use GitHub **Private Vulnerability Reporting** for this repository when available.

If private vulnerability reporting is unavailable, contact the maintainers privately before publicly disclosing technical details.

A useful report should include:

* A clear description of the vulnerability.
* The affected version or commit.
* The affected route, API endpoint, module, template, event handler, or integration.
* Preconditions required to exploit the issue.
* Reproduction steps.
* A minimal proof of concept where appropriate.
* Expected behavior.
* Observed behavior.
* Potential impact.
* Whether payments, credentials, privacy, displayed content, or connected automation may be affected.
* Relevant logs with secrets removed.
* Any proposed mitigation or patch.

Never include real:

* LNbits admin keys;
* LNbits wallet keys;
* invoice keys;
* API tokens;
* Nostr private keys;
* Lightning node credentials;
* database credentials;
* session secrets;
* access tokens;
* private wallet information

in a vulnerability report.

# Security Model

`cyberherd_extension` crosses several distinct trust boundaries:

```text
Internet / Lightning / Nostr / users
                 ↓
          input validation
                 ↓
        cyberherd_extension
          ↓       ↓       ↓
       LNbits   storage   messaging
          ↓                 ↓
      Lightning        browser / OBS
                              ↓
                         automation
```

Data entering from any external boundary should be considered untrusted unless its authenticity and authorization have been explicitly verified.

Particular care must be taken when information crosses from:

**observation or message → payment attribution → trusted event → side effect**

A user-controlled message, invoice field, Nostr event, HTTP request, or frontend parameter must never become authorization for a privileged operation merely because it parsed successfully.

# Security Scope

The following vulnerability classes are especially important.

## Lightning Payment Authentication

Payment-related behavior must derive from authoritative LNbits or Lightning payment state.

Security issues include:

* treating an unpaid invoice as paid;
* spoofing a payment notification;
* accepting a fabricated payment hash;
* accepting an unverified webhook or callback as proof of payment;
* trusting browser-provided payment status;
* incorrect association between a payment and user;
* incorrect association between a payment and message;
* processing the same payment more than once;
* triggering an action before payment settlement;
* allowing one payment to authorize multiple unintended actions;
* amount or unit confusion involving sats and millisats.

The extension should treat the Lightning backend or LNbits payment record as the authoritative source of settlement state.

Client-side state must never be considered authoritative proof of payment.

# Payment Idempotency

Every settled Lightning payment should produce its intended effects **at most once**, unless the feature explicitly defines otherwise.

Examples of dangerous duplicate effects include:

* displaying the same paid message multiple times;
* adding payment value multiple times;
* incrementing daily totals more than once;
* triggering the feeder multiple times;
* sending duplicate notifications;
* publishing duplicate external events.

Handlers must assume that:

* webhooks can be retried;
* plugin events can be delivered again;
* connections can fail after processing;
* processes can restart;
* a caller may retry after a timeout.

A retry must not automatically mean that the underlying event did not already succeed.

Where practical, use a stable identity such as:

* payment hash;
* checking ID;
* internal event ID;
* other unique settlement identifier

to enforce idempotency.

Idempotency state should survive application restart when duplicate financial or physical effects would otherwise be possible.

# Lightning Units

Lightning applications commonly mix:

* BTC;
* satoshis;
* millisatoshis;
* fiat reference values.

Financial units must be explicit.

Particular care should be given to:

```text
1 sat = 1000 msat
```

Never silently assume that an integer represents sats when an API returns millisats, or vice versa.

Amount limits should be checked:

* before invoice creation;
* after decoding external values where applicable;
* before triggering payment-dependent actions.

Overflow, underflow, negative values, unexpected zero values, and extreme values must be rejected where they are not meaningful.

# LNURL Security

LNURL-related endpoints are public Internet interfaces and must be treated accordingly.

Relevant vulnerabilities include:

* incorrect `minSendable` or `maxSendable` validation;
* accepting amounts outside advertised limits;
* incorrect msat/sat conversion;
* callback parameter manipulation;
* metadata inconsistencies;
* injection through comments or identifiers;
* forged payment association;
* trusting caller-provided settlement state;
* malformed callback responses;
* privacy leakage.

If LNURL comments are supported, comments are untrusted user input.

Supporting arbitrary text does not imply permission to render that text as HTML.

# Lightning Address Security

When exposing a Lightning Address through LNURL-pay, the well-known endpoint and callback must remain consistent.

Security-sensitive behavior includes:

* redirecting payment callbacks to attacker-controlled destinations;
* allowing host-header manipulation to change generated callbacks;
* exposing internal service addresses;
* inconsistent metadata between discovery and callback requests;
* using an untrusted request header as an authoritative external URL.

Production deployments should use an explicitly configured canonical public origin where possible.

# LNbits Authorization

LNbits API credentials are security-sensitive.

The extension should follow least privilege wherever LNbits provides distinct credentials or authorization levels.

Never expose LNbits administrative or wallet credentials to:

* browser JavaScript;
* public API responses;
* HTML templates;
* OBS browser sources;
* logs;
* Nostr events;
* error messages.

Server-side credentials must remain server-side.

Routes that modify protected extension state must verify authorization independently of the frontend.

Hiding a control in the UI is not authorization.

# Web Application Security

All HTTP inputs are untrusted.

Validate:

* path parameters;
* query parameters;
* JSON bodies;
* form inputs;
* headers;
* uploaded content;
* callback values.

Use strict schemas where possible.

Reject unknown or malformed security-sensitive fields rather than attempting to guess their intended meaning.

# Cross-Site Scripting

`cyberherd_extension` handles content that may ultimately appear in a browser or OBS Browser Source.

This makes cross-site scripting particularly important.

Potentially hostile content includes:

* payer messages;
* comments;
* usernames;
* Lightning Address identifiers;
* Nostr profile fields;
* Nostr messages;
* external API content;
* configurable announcement text.

User-controlled strings should be rendered as text, not interpreted as HTML.

Avoid inserting untrusted content with mechanisms equivalent to:

```javascript
element.innerHTML = untrustedValue
```

Prefer safe text APIs such as:

```javascript
element.textContent = untrustedValue
```

where applicable.

If HTML is intentionally supported, sanitize it with a well-maintained allowlist-based sanitizer.

# OBS and Browser Overlay Security

Browser overlays should be treated as public presentation clients, not trusted control interfaces.

An OBS Browser Source must not receive:

* LNbits administrative keys;
* wallet API keys;
* Nostr private keys;
* privileged bearer tokens;
* unrestricted management credentials.

Overlay clients should receive only the minimum data required for presentation.

A compromised browser source should not automatically grant access to the Lightning wallet or extension administration.

# Message Injection

Paid messages and informational messages may be displayed publicly.

Security controls should address:

* HTML injection;
* JavaScript injection;
* CSS injection;
* terminal/control characters;
* Unicode control characters where harmful;
* excessive message length;
* newline abuse;
* URL injection;
* message queue exhaustion.

Messages should have explicit length limits.

Display systems should treat them as plain text unless a specific sanitized rich-text format is intentionally supported.

# Message Queue Integrity

If messages are queued for display, queue processing should guarantee predictable ordering and isolation.

A newly arriving message should not corrupt or overwrite another active message unless that behavior is intentional.

Security and reliability issues include:

* unbounded queue growth;
* duplicate queue entries;
* concurrent display of messages intended to be serialized;
* starvation of important messages;
* malformed messages blocking subsequent messages.

Queues influenced by public users should have explicit bounds or retention rules.

# Feeder and Physical Automation Safety

If payment events can ultimately activate physical equipment, that boundary must be treated as safety-sensitive.

A message, HTTP request, frontend event, or Nostr event should not directly trigger physical automation unless it satisfies the intended authorization path.

For payment-triggered feeding, the general relationship should be:

```text
verified settled payment
        ↓
validated eligibility
        ↓
idempotency check
        ↓
rate / policy check
        ↓
feeder action
        ↓
record completion
```

Security-sensitive controls should include:

* maximum trigger frequency;
* duplicate suppression;
* cooldown or rate limiting;
* explicit enable/disable control;
* bounded activation duration;
* failure-safe behavior.

An attacker should not be able to activate the feeder repeatedly merely by replaying a previously valid request.

Physical safety controls should not depend exclusively on public web application state.

# Feeder Overrides

Administrative feeder overrides are privileged operations.

They must not be authorized solely by:

* knowledge of an endpoint URL;
* a frontend button;
* caller-provided state;
* easily guessable query parameters.

Override functionality should require authenticated administrative authorization.

Override state changes should be logged without exposing credentials.

# Nostr Security

Nostr events must be considered untrusted until their signatures and intended semantics are verified.

When accepting Nostr events:

* verify the event signature;
* validate the event ID;
* validate the event kind;
* validate expected tags;
* apply message-size limits;
* enforce appropriate timestamp policy;
* apply replay protection where required.

A valid Nostr signature proves control of a key.

It does **not** automatically prove that the signer is authorized to perform a privileged CyberHerd action.

Authentication and authorization are separate decisions.

# Nostr Private Keys

Nostr private keys must never be:

* sent to browsers;
* embedded in JavaScript;
* logged;
* exposed through API responses;
* included in exception messages;
* committed to source control.

Where server-side event signing is required, private keys should be loaded through protected configuration or secret storage.

# Replay Attacks

Any signed or authenticated external event capable of creating a side effect should be evaluated for replay risk.

A cryptographically valid old message may still be unsafe to execute again.

Where applicable, use:

* unique event IDs;
* timestamps;
* expiration;
* persisted replay records;
* payment hashes;
* idempotency keys.

# Webhook Security

If external services invoke webhook endpoints, do not assume that possession of the URL proves authenticity.

Where supported, validate:

* cryptographic signatures;
* shared secrets;
* expected event structure;
* timestamp freshness;
* replay identifiers.

Webhook processing must still be idempotent even when authentication is correct.

# Authentication and Sessions

Administrative routes must require appropriate authentication.

Security-sensitive session behavior should include:

* secure cookies;
* HTTP-only cookies where applicable;
* appropriate SameSite policy;
* session expiration;
* protection from session fixation;
* CSRF protection for cookie-authenticated state-changing operations.

Authentication should fail closed.

# CSRF

State-changing routes authenticated using browser cookies should be protected against Cross-Site Request Forgery.

Particular attention should be paid to:

* configuration changes;
* wallet-related operations;
* feeder overrides;
* deleting or modifying records;
* changing extension settings;
* administrative actions.

GET requests should not perform privileged state changes.

# CORS

Do not use unrestricted CORS for privileged endpoints.

A configuration equivalent to:

```text
Access-Control-Allow-Origin: *
```

should not be combined with privileged authenticated APIs.

Public read-only overlay endpoints and administrative endpoints should have separate security assumptions.

# Server-Side Request Forgery

If the extension fetches external URLs, those URLs must not permit arbitrary server-side network access.

Potential targets include:

* localhost;
* LNbits internal services;
* Lightning node RPC endpoints;
* cloud metadata services;
* RFC1918/private networks;
* container-internal services.

Do not provide unrestricted URL-fetch behavior from user-controlled input.

# Database and Persistence Security

Persisted extension state may include financially or operationally significant information.

Database logic should account for:

* duplicate records;
* race conditions;
* incomplete transactions;
* process crashes;
* migrations;
* stale records;
* malformed historical data.

Security-sensitive state should include appropriate uniqueness constraints where possible.

Examples may include uniqueness for:

* payment identifiers;
* processed trigger events;
* replay-prevention identifiers.

Do not rely solely on an in-memory set to prevent duplicate irreversible actions if that protection must survive restart.

# Concurrency

Concurrent payment and message processing can create security problems even without traditional data corruption.

Potential races include:

```text
request A → check "not processed"
request B → check "not processed"
request A → perform action
request B → perform action
```

The check and state transition for irreversible actions should be atomic or otherwise concurrency-safe.

Database uniqueness constraints, transactions, locks, or equivalent primitives should be used where appropriate.

# Input Size Limits

Public endpoints should apply reasonable limits to:

* message length;
* comment length;
* request-body size;
* arrays;
* metadata;
* Nostr events;
* queued messages.

Never permit untrusted users to create unlimited persistent data.

# Rate Limiting

Public or expensive endpoints should be evaluated for rate limiting.

Examples include:

* LNURL endpoints;
* message submission;
* invoice creation;
* Nostr ingestion;
* lookup endpoints;
* feeder-related requests;
* websocket connection establishment.

Rate limiting should complement authorization and validation, not replace them.

# WebSockets and Live Updates

If WebSockets or similar real-time mechanisms are used, clients should not gain privileges merely by establishing a connection.

Validate:

* subscription scope;
* authentication when needed;
* message structure;
* maximum message size;
* connection count;
* reconnect behavior.

Do not broadcast secrets or private wallet information to generic subscribers.

# Sensitive Data and Privacy

Collect and retain only data required for the extension's functionality.

Potentially sensitive information may include:

* Lightning payment identifiers;
* payer messages;
* Nostr public keys;
* IP addresses;
* wallet identifiers;
* timestamps;
* user metadata.

Public display and persistent storage are different decisions.

Do not assume that information received with a payment is automatically appropriate for indefinite retention or public display.

# Logging

Logs should provide enough information to investigate significant operations without leaking credentials.

Useful fields may include:

* payment identifier;
* event identifier;
* trigger type;
* processing result;
* idempotency result;
* authorization result;
* suppression reason.

Logs must not include:

* LNbits administrative keys;
* wallet keys;
* Nostr private keys;
* passwords;
* bearer tokens;
* session secrets;
* complete authentication headers.

Sanitize external response bodies before logging when they may contain secrets or user-controlled content.

# Error Handling

Public errors should reveal enough information to diagnose normal usage problems without exposing internal security details.

Avoid returning:

* stack traces;
* filesystem paths;
* SQL statements;
* API keys;
* environment variables;
* internal network addresses;
* complete upstream responses containing secrets.

Detailed diagnostics can be written to protected server logs when appropriate.

# Secrets

Secrets should be provided through appropriate server-side configuration or secret-management mechanisms.

Never commit real secrets to Git.

The repository should not contain production:

```text
LNbits admin keys
wallet API keys
Nostr private keys
Lightning credentials
database passwords
API access tokens
session secrets
```

Example configuration should use obvious placeholders.

# Dependencies and Supply Chain

Python, JavaScript, LNbits, and frontend dependencies are part of the trusted computing base.

Dependency changes should be reviewed for:

* maintenance status;
* known vulnerabilities;
* unexpected transitive dependencies;
* install-time scripts;
* ownership changes;
* unnecessary privileges.

Use dependency locking where supported.

Automated dependency updates should still undergo testing before production deployment.

# GitHub Actions

GitHub Actions workflows should follow least privilege.

Prefer explicit permissions such as:

```yaml
permissions:
  contents: read
```

and grant additional permissions only to jobs requiring them.

Avoid exposing repository secrets to untrusted pull-request code.

Pin security-sensitive third-party Actions to trusted versions or commit SHAs where practical.

# Fail-Safe Behavior

When payment or authorization state is ambiguous, the preferred behavior is:

**do not perform the privileged side effect.**

Examples include:

* settlement cannot be confirmed;
* payment identity is missing;
* idempotency state cannot be checked;
* database transaction fails;
* message validation fails;
* feeder authorization cannot be verified;
* configuration is inconsistent.

Public display or read-only operation may continue when safe.

Do not convert an internal validation failure into an assumption of success.

# Testing Expectations

Security-sensitive functionality should include tests covering:

* duplicate payment notifications;
* replayed events;
* simultaneous payment processing;
* invalid amounts;
* sat/msat boundaries;
* malformed LNURL requests;
* unpaid invoices;
* XSS payloads;
* HTML injection;
* oversized messages;
* invalid Nostr signatures;
* unauthorized Nostr events;
* expired/replayed events;
* unauthorized administrative requests;
* CSRF-sensitive operations;
* feeder duplicate suppression;
* process restart during event processing;
* database failures.

Where feasible, payment integration testing should use a controlled LNbits and Lightning test environment rather than production funds.

# Security Invariants

Important invariants should be expressible and tested.

Examples include:

```text
one settled payment → no more than one feeder trigger
```

```text
unpaid payment → zero payment-authorized side effects
```

```text
untrusted message → never executable HTML/JavaScript
```

```text
public browser client → never receives wallet secrets
```

```text
invalid Nostr signature → zero privileged side effects
```

```text
valid Nostr signature + unauthorized key → zero privileged side effects
```

```text
duplicate event → no duplicate irreversible effect
```

# Production Deployment

Recommended production practice:

1. Run a maintained LNbits release.
2. Review extension changes before upgrading.
3. Back up extension and LNbits data.
4. Run the test suite.
5. Protect administrative credentials.
6. Use HTTPS for public deployments.
7. Place LNbits behind an appropriately configured reverse proxy.
8. Restrict administrative interfaces where practical.
9. Monitor payment and feeder event logs.
10. Maintain a straightforward way to disable feeder integration independently of payment reception.

New physical or financial automation should first be tested without real irreversible effects where practical.

# Out of Scope

The following normally do not constitute vulnerabilities:

* expected Lightning payment fees;
* normal Lightning payment failure;
* messages that are offensive but do not bypass technical controls;
* rate differences caused by ordinary Lightning routing;
* vulnerabilities only affecting unsupported versions;
* social engineering without a software vulnerability;
* generic dependency CVEs with no demonstrated impact;
* theoretical attacks without a plausible execution path;
* automated scanner results without validation.

Spam, abusive messages, or undesirable public content may be operational problems rather than security vulnerabilities unless they bypass intended controls or permit injection, denial of service, privilege escalation, or another security impact.

# Responsible Disclosure

We appreciate researchers who:

* report vulnerabilities privately;
* provide reproducible reports;
* avoid accessing funds or private information beyond what is necessary;
* avoid triggering physical hardware unnecessarily;
* avoid disrupting the public Lightning Goats service;
* use test environments where practical;
* give maintainers reasonable time to remediate;
* coordinate disclosure where users may need to update.

Security testing must not intentionally:

* steal Bitcoin;
* spend another user's funds;
* drain LNbits wallets;
* repeatedly activate connected equipment;
* interfere with animal care;
* expose private user data;
* degrade production Lightning infrastructure.

# Security Is a Process

`cyberherd_extension` sits at the intersection of Internet-facing software, Lightning payments, user-generated content, public broadcasting, and potentially physical automation.

Security therefore depends on more than conventional web application protections.

It requires:

* authoritative payment verification;
* idempotent event handling;
* strict trust boundaries;
* safe rendering;
* secure secret management;
* replay protection;
* authentication and authorization;
* conservative physical automation;
* operational monitoring.

If you identify behavior that could spoof a payment, produce duplicate payment effects, activate equipment without authorization, expose credentials, execute injected browser content, bypass administrative controls, or otherwise compromise the LNbits instance or its users, please report it privately.
