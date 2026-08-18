# reifydb-dev

A general developer/debug inspector for **stopped** ReifyDB instances. It provides two lenses -
`catalog` and `cdc` - which attribute on-disk bytes and rows to the logical objects you
actually reason about (tables, views, series, ringbuffers, flow operators) instead of the opaque
physical tables SQLite stores. More inspection subcommands are expected to be added over time.

## Safety rules (read first)

- **Run only against a throwaway copy of a stopped database directory.** The naming half boots
  the embedded engine to read the catalog, and booting WRITES to the directory (pragmas,
  bootstrap, CDC). Never point this at a live instance or an original snapshot you care about.
- **Version-coupled.** Build `reifydb-dev` from the same reifydb commit that produced the
  snapshot. The catalog and flow-node decoders are tied to the on-disk format and will misread
  silently if the versions diverge.

## Subcommands

- `catalog <dir>` - dump the id -> name map for every object kind (no sizing).
- `cdc <dir>` - decode every row in `cdc.db` and attribute its bytes to source objects, change
  kinds, and system key kinds.

`<dir>` is the path to the already-copied, stopped sqlite database directory (the one
containing `multi.db` / `operator.db`).

## Usage

Run via cargo (package name is `dev`, binary is `reifydb-dev`):

```sh
# Dump the id -> name reference
cargo run -p dev -- catalog /path/to/db-copy

# What is actually inside cdc.db
cargo run -p dev -- cdc /path/to/db-copy

# Every origin and key kind, no truncation
cargo run -p dev -- cdc /path/to/db-copy --all

# Decode-only: never boots the engine, so it is safe on any snapshot (ids stay numeric)
cargo run -p dev -- cdc /path/to/db-copy --no-names
```

Or run the built binary directly:

```sh
cargo build -p dev --release
./target/release/reifydb-dev cdc /path/to/db-copy
```

## Flags

### `catalog`

| Flag | Description |
| --- | --- |
| `<dir>` | Path to the already-copied, stopped sqlite database directory. |
| `--json` | Emit JSON lines instead of a listing. |

### `cdc`

| Flag | Description |
| --- | --- |
| `<dir>` | Path to the already-copied, stopped sqlite database directory. |
| `--all` | Show every origin and key kind (default: only the top `--top` rows). |
| `--top <N>` | Show at most N rows per table (default: 40). |
| `--no-names` | Skip the catalog boot; ids stay numeric but nothing is written to the directory. |
| `--no-blocks` | Only scan the live `cdc` table, ignore `cdc_block`. |
| `--json` | Emit JSON lines instead of tables. |

## How it works

- **`catalog.rs` (naming, boots the engine).** Opens the directory through the embedded engine
  and queries `system::namespaces`, `system::tables`, `system::series`, `system::ringbuffers`,
  `system::views`, `system::flows`, and `system::operators` as root (bypassing the `system::*`
  policy gate), then always stops the engine. It builds two maps: source id -> logical name, and
  operator flow-node id -> a `view  [stage]{operator}` label. A view owns its rows under its own
  id, so it names itself and needs no join through a backing object. The operator label is
  decoded from the flow node `data` blob: the first byte is a `FlowNodeType` discriminant
  (indexed into the local `NODE_TYPE` table) and `Apply` nodes carry their operator name as an
  embedded string.

- **`report.rs` (render).** Renders the catalog dump and the CDC breakdown, each with a `--json`
  variant emitting one JSON object per line. Objects with no catalog match render as `(unmapped)`.

- **`cdc.rs` (`cdc.db` content breakdown).** `cdc.db` is the CDC log, not `multi.db`: one row per
  commit, holding two blobs. `payload` is `zstd-1(postcard(Cdc))` and is decoded with
  `reifydb-codec`'s `cdc` module, the same codec the write path uses, so the tool cannot drift;
  `cdc_block` rows (present only if compaction ran) decode as a `Vec<Cdc>` through it. `stats_rollup`
  is a second, uncompressed postcard blob per row carrying `Vec<CdcEviction>`, and is sized
  separately because nothing else attributes it. Byte attribution re-encodes each `SystemChange`
  with postcard and measures the result, so per-object bytes sum to the uncompressed payload
  rather than to compressed on-disk bytes: the compression ratio is reported once, globally,
  since zstd is applied per commit and cannot be split per origin. `system_changes` is the only
  stored stream, so the per-object breakdown is derived the way `reifydb-cdc`'s rebuild derives
  it: `rebuild::row_target` maps an encoded row key to its owning `ObjectId`, and
  `rebuild::changed_objects` gives the touched set. The report counts exactly what the rebuild
  emits: inserts, updates and visible deletes. A TTL removal carrying `visible: false` is skipped
  here as it is there, so the two never disagree; its bytes still land in the system total.
  Non-row system changes are bucketed by `Key::kind` on the encoded key, which must go through
  that helper because keys are keycode-inverted and the raw first byte is not the tag.

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
