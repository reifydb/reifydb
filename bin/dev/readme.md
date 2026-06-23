# reifydb-dev

A general developer/debug inspector for **stopped** ReifyDB instances. Today it provides one
lens, `storage`, which attributes on-disk bytes and rows to the logical objects you actually
reason about (tables, views, series, ringbuffers, flow operators) instead of the opaque
physical `source_<id>` / `operator_<id>` tables SQLite stores. More inspection subcommands are
expected to be added over time; `storage` is just the first.

## Safety rules (read first)

- **Run only against a throwaway copy of a stopped database directory.** The naming half boots
  the embedded engine to read the catalog, and booting WRITES to the directory (pragmas,
  bootstrap, CDC). Never point this at a live instance or an original snapshot you care about.
- **Sizing is read-only.** The `dbstat` half opens `multi.db` strictly read-only and never
  mutates it; only the engine boot writes.
- **Version-coupled.** Build `reifydb-dev` from the same reifydb commit that produced the
  snapshot. The catalog and flow-node decoders are tied to the on-disk format and will misread
  silently if the versions diverge.

## Subcommands

- `storage <dir>` - attribute on-disk bytes/rows to logical objects (`namespace::name`), by
  joining `dbstat` sizing with the real `system::*` catalog.
- `catalog <dir>` - dump the id -> name map for every object kind (no sizing).

`<dir>` is the path to the already-copied, stopped sqlite database directory (the one
containing `multi.db`).

## Usage

Run via cargo (package name is `dev`, binary is `reifydb-dev`):

```sh
# Top objects by total on-disk size
cargo run -p dev -- storage /path/to/db-copy

# Roll up by namespace, show everything
cargo run -p dev -- storage /path/to/db-copy --group-by namespace --all

# Roll up by storage tier (current / version-index / historical)
cargo run -p dev -- storage /path/to/db-copy --group-by tier

# Exact row counts (slow) and JSON output
cargo run -p dev -- storage /path/to/db-copy --exact-rows --json

# Only objects whose logical name contains "orders"
cargo run -p dev -- storage /path/to/db-copy --filter orders

# Dump the id -> name reference
cargo run -p dev -- catalog /path/to/db-copy
```

Or run the built binary directly:

```sh
cargo build -p dev --release
./target/release/reifydb-dev storage /path/to/db-copy
```

## Flags

### `storage`

| Flag | Description |
| --- | --- |
| `<dir>` | Path to the already-copied, stopped sqlite database directory. |
| `--group-by <namespace\|tier>` | Roll up by dimension instead of listing each object. |
| `--all` | Show every object (default: only the top `--top` rows). |
| `--top <N>` | Show at most N objects (default: 40). |
| `--filter <substr>` | Only show objects whose logical name contains this substring. |
| `--exact-rows` | Replace the fast `dbstat` row estimate with an exact `COUNT(*)` per table (slow). |
| `--no-rows` | Do not compute row counts at all. |
| `--json` | Emit JSON lines instead of a table. |

### `catalog`

| Flag | Description |
| --- | --- |
| `<dir>` | Path to the already-copied, stopped sqlite database directory. |
| `--json` | Emit JSON lines instead of a listing. |

## How it works

The tool joins two independent data sources: sizing from SQLite, names from ReifyDB's catalog.

- **`dbstat.rs` (sizing, read-only).** Opens `multi.db` read-only and runs a single query
  against SQLite's builtin `dbstat` virtual table. Bytes per physical table come from summing
  `pgsize`; the fast row estimate counts leaf-page cells (a WITHOUT ROWID table stores one row
  per leaf cell), avoiding a full scan. With `--exact-rows` it additionally runs `COUNT(*)` per
  `*_<id>__current` table. Each physical table is classified into `(kind, id, tier)` where kind
  is `source` or `operator` and tier is current / `__version` (the MVCC version index) /
  `__historical`.

- **`catalog.rs` (naming, boots the engine).** Opens the directory through the embedded engine
  and queries `system::namespaces`, `system::tables`, `system::series`, `system::ringbuffers`,
  `system::views`, `system::flows`, and `system::flow_nodes` as root (bypassing the `system::*`
  policy gate), then always stops the engine. It builds two maps: physical source id -> logical
  name (a deferred view maps via its `underlying_id`, since its materialized rows live in the
  underlying storage shape), and operator flow-node id -> a `view  [stage]{operator}` label. The
  operator label is decoded from the flow node `data` blob: the first byte is a `FlowNodeType`
  discriminant (indexed into the local `NODE_TYPE` table) and `Apply` nodes carry their operator
  name as an embedded string.

- **`report.rs` (join + render).** Joins sizing with names and renders in one of three modes:
  per-object (default, sorted by total bytes with a `--top`/`--all` cutoff), `--group-by
  namespace`, or `--group-by tier`. Every mode also has a `--json` variant emitting one JSON
  object per line. Objects with no catalog match render as `(unmapped)`.

- **`context.rs` (shared handles).** `Context` holds handles shared across subcommands -
  currently just the `Clock` used for timing. It is the seam future inspectors share.

## Limitations / notes

- `NODE_TYPE` in `catalog.rs` mirrors the `FlowNodeType` declaration order in
  `crates/rql/src/flow/node.rs` by hand; if that enum's order changes, operator labels here go
  wrong until it is re-synced.
- Physical tables with no catalog entry are shown as `(unmapped)` rather than dropped.
- `reifydb-dev --help` lists argument names without descriptions; the per-flag documentation
  lives in this readme instead.
