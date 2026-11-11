# reifydb-dev

A general developer/debug inspector for **stopped** ReifyDB instances. It provides one lens -
`catalog` - which attributes on-disk bytes and rows to the logical objects you
actually reason about (tables, views, series, ringbuffers, flow operators) instead of the opaque
physical tables SQLite stores. More inspection subcommands are expected to be added over time.

## Safety rules (read first)

- **Run only against a throwaway copy of a stopped database directory.** The naming half boots
  the embedded engine to read the catalog, and booting WRITES to the directory (pragmas,
  bootstrap, commit log). Never point this at a live instance or an original snapshot you care about.
- **Version-coupled.** Build `reifydb-dev` from the same reifydb commit that produced the
  snapshot. The catalog and flow-node decoders are tied to the on-disk format and will misread
  silently if the versions diverge.

## Subcommands

- `catalog <dir>` - dump the id -> name map for every object kind (no sizing).

`<dir>` is the path to the already-copied, stopped sqlite database directory (the one
containing `multi.db` / `operator.db`).

## Usage

Run via cargo (package name is `dev`, binary is `reifydb-dev`):

```sh
# Dump the id -> name reference
cargo run -p dev -- catalog /path/to/db-copy
```

Or run the built binary directly:

```sh
cargo build -p dev --release
./target/release/reifydb-dev catalog /path/to/db-copy
```

## Flags

### `catalog`

| Flag | Description |
| --- | --- |
| `<dir>` | Path to the already-copied, stopped sqlite database directory. |
| `--json` | Emit JSON lines instead of a listing. |

## How it works

- **`catalog.rs` (naming, boots the engine).** Opens the directory through the embedded engine
  and queries `system::namespaces`, `system::tables`, `system::series`, `system::ringbuffers`,
  `system::views`, `system::flows`, and `system::flow::operators` as root (bypassing the `system::*`
  policy gate), then always stops the engine. It builds two maps: source id -> logical name, and
  operator flow-node id -> a `view  [kind]` label. A view owns its rows under its own
  id, so it names itself and needs no join through a backing object. The operator label is
  the `kind` column of `system::flow::operators`.

- **`report.rs` (render).** Renders the catalog dump, with a `--json`
  variant emitting one JSON object per line. Objects with no catalog match render as `(unmapped)`.

- **`context.rs` (shared handles).** `Context` holds handles shared across subcommands -
  currently just the `Clock` used for timing. It is the seam future inspectors share.

## Limitations / notes

- `NODE_TYPE` in `catalog.rs` mirrors the `FlowNodeType` declaration order in
  `crates/rql/src/flow/node.rs` by hand; if that enum's order changes, operator labels here go
  wrong until it is re-synced.
- Objects with no catalog entry are shown as `(unmapped)` rather than dropped.
- There is no on-disk sizing lens. The `storage` subcommand and its `dbstat` reader were removed;
  physical byte attribution now has to come from SQLite directly.
- `reifydb-dev --help` lists argument names without descriptions; the per-flag documentation
  lives in this readme instead.
