# Fuzz Testing

## Prerequisites

- Rust **nightly** toolchain: `rustup toolchain install nightly`
- `cargo-fuzz`: `cargo install cargo-fuzz`

## Make Targets

| Target              | Description                                        |
|---------------------|----------------------------------------------------|
| `make fuzz-list`    | List all available fuzz targets                    |
| `make fuzz-run TARGET=<name>` | Run a single target (default 60s)        |
| `make fuzz-run TARGET=<name> DURATION=300` | Run with custom duration      |
| `make fuzz-smoke`   | Run every target for 10s each                      |
| `make fuzz-regression` | Replay all saved artifacts                      |

## Fuzz Targets

| Target               | What it tests                                              |
|----------------------|------------------------------------------------------------|
| `abi_buffer`         | `BufferFFI::from_slice` roundtrip and invariants           |
| `abi_layout`         | `LayoutFFI::is_defined` with arbitrary field/buffer combos |
| `keycode_deserialize`| `keycode::deserialize` on raw bytes for all scalar types   |
| `keycode_roundtrip`  | serialize-then-deserialize identity for all scalar types   |
| `rql_gen`            | (support module) Arbitrary RQL statement generator         |
| `rql_parse`          | `reifydb_rql::ast::parse_str` on generated RQL            |
| `rql_tokenize`       | `reifydb_rql::token::tokenize` on generated RQL           |
| `sql_tokenize`       | `reifydb_sql::token::tokenize` on arbitrary strings        |
| `sql_transpile`      | `reifydb_sql::transpile` on arbitrary strings              |

## Adding Regression Artifacts

When a fuzzer finds a crash or OOM, it writes an artifact file to `fuzz/artifacts/<target>/`.

To turn it into a regression test:

1. `git add fuzz/artifacts/<target>/<artifact-file>`
2. Commit the artifact.
3. `make fuzz-regression` replays every artifact under `fuzz/artifacts/` against its target — directory name must match the target name.
