# ReifyDB Uptime

Self-hostable, multi-user uptime monitoring built on ReifyDB. Runs as a single
binary that embeds a ReifyDB server, an HTTP API, the web UI, and the check
worker. Deployed publicly at uptime.reifydb.com.

## Features

- HTTP(S) checks (status code, response time, optional keyword match)
- TCP port checks
- ICMP ping checks (unprivileged datagram sockets)
- DNS resolution checks (optional expected IP)
- Multi-user accounts (email + password); users are ReifyDB identities and
  sessions are ReifyDB tokens
- Public status pages at `/status/<slug>`
- All state lives in ReifyDB (`uptime` namespace, RQL migrations at startup)

## Quick start

```sh
cd webapp && pnpm install && pnpm build && cd ..
cargo run -p reifydb-uptime
```

Open http://localhost:8080, register an account, and create a monitor.
Without `webapp/dist`, the binary still builds and serves a placeholder page.

For UI development run `pnpm dev` in `webapp/` (Vite on :5173, proxying `/api`
and `/db` to :8080) while the binary is running.

## Configuration

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--http-bind` | `UPTIME_HTTP_BIND` | `0.0.0.0:8080` | UI + API + public status pages |
| `--reifydb-http-bind` | `UPTIME_REIFYDB_HTTP_BIND` | `127.0.0.1:8090` | ReifyDB HTTP subsystem (auth forward target) |
| `--reifydb-ws-bind` | `UPTIME_REIFYDB_WS_BIND` | `127.0.0.1:8091` | ReifyDB WebSocket subsystem |
| `--data-dir` | `UPTIME_DATA_DIR` | `./data` | SQLite storage directory |
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
