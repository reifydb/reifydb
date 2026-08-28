# ReifyDB Uptime

Self-hostable, multi-user uptime monitoring built on ReifyDB. Runs as a single
binary that embeds a ReifyDB server, an HTTP API, the web UI, and the check
worker. Deployed publicly at uptime.reifydb.com.

## Features

- HTTP(S) checks (status code, response time, optional keyword match)
- TCP port checks
- ICMP ping checks (unprivileged datagram sockets)
- DNS resolution checks (optional expected IP)
- No signup wall: every visitor gets a guest identity and can create monitors
  right away, then registers to keep them
- Multi-user accounts (email + password); users are ReifyDB identities and
  sessions are ReifyDB tokens
- Public status pages at `/status/<slug>`
- All state lives in ReifyDB (`uptime` namespace, RQL migrations at startup)

## Quick start

```sh
cd web && pnpm install && pnpm build && cd ..
cargo run -p reifydb-uptime-backend
```

Open http://localhost:8080 and create a monitor - no account needed.
Without `web/dist`, the binary still builds and serves a placeholder page.

For UI development run `pnpm dev` in `web/` (Vite on :5173, proxying `/api`
and `/db` to :8080) while the binary is running. `pnpm dev:local` does the same
but resolves the `@reifydb/*` packages from the local sources in
`pkg/typescript` instead of the published npm packages.

## Configuration

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--http-bind` | `UPTIME_HTTP_BIND` | `0.0.0.0:8080` | UI + API + public status pages |
| `--reifydb-http-bind` | `UPTIME_REIFYDB_HTTP_BIND` | `127.0.0.1:8090` | ReifyDB HTTP subsystem (auth forward target) |
| `--reifydb-ws-bind` | `UPTIME_REIFYDB_WS_BIND` | `127.0.0.1:8091` | ReifyDB WebSocket subsystem |
| `--data-dir` | `UPTIME_DATA_DIR` | `/tmp/uptime` | SQLite storage directory |
| `--max-concurrent-checks` | `UPTIME_MAX_CONCURRENT_CHECKS` | `64` | Check fan-out limit |
| `--allow-private-targets` | `UPTIME_ALLOW_PRIVATE_TARGETS` | off | Permit monitors that resolve to private/loopback ranges |
| `--memory` | | off | In-memory storage (demo/tests, no persistence) |

## Architecture

One process, three parts, all on the ReifyDB runtime:

- ReifyDB in server mode (SQLite storage) with HTTP and WS subsystems bound to
  loopback. Schema is bootstrapped through ReifyDB migrations.
- An Axum server serving the embedded React UI, the `/api` endpoints, and a
  `/db/v1/authenticate` + `/db/v1/logout` forward to the ReifyDB HTTP
  subsystem, so the browser talks to ReifyDB auth same-origin.
- A scheduler loop that queries due monitors every 2 seconds and fans out
  checks, recording results with conflict-retrying RQL commands.

Authentication is ReifyDB's own: registration executes `CREATE USER` plus a
`password` authentication method (argon2id), login mints an opaque ReifyDB
session token, and the API validates bearer tokens against the ReifyDB
catalog. The web UI signs in through `@reifydb/auth`'s password flow.

## Guest mode

The first page load provisions a guest: `POST /api/auth/guest` creates a real
ReifyDB identity of kind `guest` (named `guest:<uuid7>`) and mints a 30-day
session token, which the browser adopts through `@reifydb/auth`. A guest owns
monitors and status pages exactly like a registered user - the `owner ==
$identity.id` policies make no distinction - and the UI shows a primary-color
bar with the CTA to register.

Registering while holding a guest token **promotes that identity in place**: one
admin transaction renames it to the email, flips its kind to `user`, records the
`email` attribute, and attaches the password credential. The `IdentityId` never
changes, so every monitor, result and status page stays owned by the same
principal - nothing is copied or re-created. Registering without a guest token
creates a fresh account as before.

Guests hold no credential, so their session cannot be re-established once the
browser storage is gone; the UI therefore offers a guest "Create account" and
"Sign in" instead of "Sign out". Guest quotas are not implemented yet.

Only port 8080 needs to be exposed; terminate TLS in a reverse proxy.

## ICMP ping notes

Ping uses unprivileged datagram ICMP sockets. On Linux the process group must
be allowed by the kernel:

```sh
sysctl -w net.ipv4.ping_group_range="0 2147483647"
```

If the socket cannot be created, ping checks record a failure result with an
explanatory error instead of crashing.

## SSRF guard

Monitors whose target resolves to loopback, private, link-local, CGNAT, or
unique-local ranges are rejected at check time unless the instance runs with
`--allow-private-targets`. Self-hosters monitoring their own LAN should enable
it; the public deployment must not.
