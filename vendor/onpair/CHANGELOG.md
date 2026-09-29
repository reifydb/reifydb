# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/spiraldb/onpair/compare/v0.2.0...HEAD)

### Added

- Add row-based training and compression without flattening input first.
- Add encoding into reusable caller-owned buffers.

### Changed

- Scan dynamic training samples sequentially without changing their contents or
  order.
- Speed up matching for large dictionaries.
- Speed up token-pair frequency counting and reserve capacity for training tables
  and matcher maps.
- Speed up compression by reserving final matcher maps for the dictionary size
  and long-token count.

### Fixed

- Train on the randomly selected rows returned by partial shuffling, expanding
  the selection when needed to cover the sampling budget.
- Ignore bytes outside the row offsets when calculating the training budget.

## [0.2.0](https://github.com/spiraldb/onpair/compare/v0.1.1...v0.2.0) - 2026-07-30

### Added

- Make `CompactDictionary` storage-backed, allowing validated dictionary bytes
  and offsets to be borrowed or shared without copying.
- Separate dictionary safety validation from correctness validation, allowing
  bounded decoding and tokenization checks without requiring full semantic
  validation.

### Changed

- Add `rust-analyzer` to the pinned Rust toolchain.

### Removed

- Remove the obsolete cross-implementation benchmark harness and standalone
  TPC-H example, retaining the Rust benchmarks under `benches/`.

### Fixed

- Ensure the Rust setup action installs the pinned toolchain without referring to a nonexistent cache step.
- Remove the vulnerable transitive `thrift` dependency by upgrading the Arrow,
  Parquet, and TPC-H benchmark dependencies.

## [0.1.1](https://github.com/spiraldb/onpair/compare/v0.1.0...v0.1.1) - 2026-07-17

### Fixed

- Use a deterministic seed in the default training configuration.
- Reject compact dictionaries containing more than 65,536 tokens, which cannot
  be addressed by the `u16` token type.
- Add regression coverage for the 65,536-token boundary and document the
  dictionary size limit in the invariants and interchange format.

## [0.1.0](https://github.com/spiraldb/onpair/compare/v0.0.4...v0.1.0) - 2026-07-14

### Added

- Add compressed-domain equality, prefix, and substring search APIs.
- Add `Column::into_raw` and `code_bits_for_num_tokens` for embedders that
  store OnPair buffers in their own layout.

### Changed

- Refactor the public API around modules, `Column`/`ColumnView`, and validated dictionary types.
- Rename the training dictionary-width knob to `MaxDictBits` / `Config::max_dict_bits`, making it explicit that it is a dictionary-size budget; runtime code width is derived from dictionary size via `CompactDictionary::code_bits`.
- Bump the crate to 0.1.0 for the breaking public API changes.

## [0.0.4](https://github.com/spiraldb/onpair/compare/v0.0.3...v0.0.4) - 2026-06-01

### Added

- add back code_offsets to compressor ([#15](https://github.com/spiraldb/onpair/pull/15))

### Other

- remove code boundaries ([#13](https://github.com/spiraldb/onpair/pull/13))

## [0.0.3](https://github.com/spiraldb/onpair/compare/v0.0.2...v0.0.3) - 2026-05-29

### Other

- fat-table layout with scalar copy and L2-indexed fallback ([#12](https://github.com/spiraldb/onpair/pull/12))
- Feat/decode fat table scalar ([#7](https://github.com/spiraldb/onpair/pull/7))
- update changelog ([#10](https://github.com/spiraldb/onpair/pull/10))

## [0.0.2](https://github.com/spiraldb/onpair/compare/v0.0.1...v0.0.2) - 2026-05-29

### Other

- automate releases with release-plz ([#8](https://github.com/spiraldb/onpair/pull/8))
- clean up benchmarks and decompression ([#6](https://github.com/spiraldb/onpair/pull/6))
- add benchmarks with onpair cpp ([#5](https://github.com/spiraldb/onpair/pull/5))
- refine the onpair public API

## [0.0.1] - 2026-05-29

### Added

- Initial pure-Rust port of the onpair short-strings compression codec ([#4](https://github.com/spiraldb/onpair/pull/4)).
- Benchmarks comparing against the onpair C++ reference implementation ([#5](https://github.com/spiraldb/onpair/pull/5)).
- TPC-H and ClickBench benchmark harnesses.
- CI workflow (build, fmt, clippy, test) and Codspeed benchmark workflow ([#1](https://github.com/spiraldb/onpair/pull/1)).

### Changed

- Cleaned up benchmarks and decompression path ([#6](https://github.com/spiraldb/onpair/pull/6)).
