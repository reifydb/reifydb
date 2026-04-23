// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::id::{SeriesId, TableId},
};
use reifydb_runtime::context::clock::Instant;

use crate::{
	bucket::{Bucket, BucketId},
	column_block::ColumnBlock,
};

// Registry key. Disjoint keyspaces per shape: table snapshots are keyed by
// `(table_id, commit_version)`, series by `(series_id, bucket)`. Bucket
// replacement reuses the same `(series_id, bucket)` key, so late-arrival
// re-materialization overwrites atomically via `DashMap::insert`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SnapshotId {
	Table {
		table_id: TableId,
		commit_version: CommitVersion,
	},
	Series {
		series_id: SeriesId,
		bucket: BucketId,
	},
}

// Provenance - what the snapshot was built from. The `Series` variant carries
// the full `Bucket` (not just `BucketId`) and the `sequence_counter` observed
// at materialization time. Readers use these to decide whether a snapshot is
// still current vs. stale.
#[derive(Clone, Debug)]
pub enum SnapshotSource {
	Table {
		table_id: TableId,
		commit_version: CommitVersion,
	},
	Series {
		series_id: SeriesId,
		bucket: Bucket,
		sequence_counter: u64,
	},
}

// A materialized columnar snapshot. Same `ColumnBlock` container for both
// shapes - table and series snapshots differ only in their provenance and
// keying.
#[derive(Clone)]
pub struct Snapshot {
	pub id: SnapshotId,
	pub source: SnapshotSource,
	pub namespace: String,
	pub name: String,
	pub created_at: Instant,
	pub block: ColumnBlock,
}

impl Snapshot {
	pub fn meta(&self) -> SnapshotMeta {
		SnapshotMeta {
			id: self.id,
			namespace: self.namespace.clone(),
			name: self.name.clone(),
			created_at: self.created_at.clone(),
			row_count: self.block.len(),
		}
	}
}

// Lightweight listing record - the shape callers get from
// `SnapshotRegistry::list()` without cloning the backing `ColumnBlock`.
#[derive(Clone, Debug)]
pub struct SnapshotMeta {
	pub id: SnapshotId,
	pub namespace: String,
	pub name: String,
	pub created_at: Instant,
	pub row_count: usize,
}
