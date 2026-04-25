// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::id::{SeriesId, TableId},
	value::column::array::Column,
};
use reifydb_runtime::context::clock::Instant;
use reifydb_type::value::r#type::Type;

use crate::bucket::{Bucket, BucketId};

// A column as a sequence of `Column` chunks, each encoded independently. v1
// materialization produces single-chunk `ColumnChunks`s; multi-chunk support
// is reserved for the future batched-scan path.
#[derive(Clone)]
pub struct ColumnChunks {
	pub ty: Type,
	pub nullable: bool,
	pub chunks: Vec<Column>,
}

impl ColumnChunks {
	pub fn new(ty: Type, nullable: bool, chunks: Vec<Column>) -> Self {
		Self {
			ty,
			nullable,
			chunks,
		}
	}

	pub fn single(ty: Type, nullable: bool, array: Column) -> Self {
		Self {
			ty,
			nullable,
			chunks: vec![array],
		}
	}

	pub fn len(&self) -> usize {
		self.chunks.iter().map(|c| c.len()).sum()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn chunk_count(&self) -> usize {
		self.chunks.len()
	}
}

pub type Schema = Arc<Vec<(String, Type, bool)>>;

// The column container used by a `Snapshot` - a schema plus one
// `ColumnChunks` per user column. The schema's tuple entries are
// `(name, ty, nullable)` in positional order.
#[derive(Clone)]
pub struct ColumnBlock {
	pub schema: Schema,
	pub columns: Vec<ColumnChunks>,
}

impl ColumnBlock {
	pub fn new(schema: Schema, columns: Vec<ColumnChunks>) -> Self {
		debug_assert_eq!(schema.len(), columns.len(), "ColumnBlock::new: schema and columns length mismatch");
		Self {
			schema,
			columns,
		}
	}

	pub fn len(&self) -> usize {
		self.columns.first().map(|c| c.len()).unwrap_or(0)
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn column_by_name(&self, name: &str) -> Option<(usize, &ColumnChunks)> {
		self.schema.iter().position(|(n, _, _)| n == name).map(|i| (i, &self.columns[i]))
	}
}

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
