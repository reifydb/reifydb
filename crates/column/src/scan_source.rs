// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_type::Result;

// Abstraction over "something that emits `Columns` batches" — the upstream
// input to materialization. The trait is defined here (pure crate) so that
// `reifydb-column` consumers can see the shape, but impls live in
// `reifydb-sub-column` where the engine's `QueryNode`-backed scan is in
// scope. Renamed from `SnapshotSource` to avoid colliding with the
// `SnapshotSource` provenance enum in `snapshot.rs`.
pub trait ScanSource {
	// Return the next batch of rows as a `Columns`, or `None` when exhausted.
	fn next_batch(&mut self) -> Result<Option<Columns>>;
}
