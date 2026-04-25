// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::id::{SeriesId, TableId},
	value::column::array::Column,
};
use reifydb_runtime::context::clock::Instant;
use reifydb_type::{Result, value::r#type::Type};

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

	// Map a row range [start, end) onto the chunks that intersect it. Each entry is
	// `(chunk_idx, intra_start, intra_end)` where `intra_*` are offsets within
	// `chunks[chunk_idx]`. Empty chunks contribute nothing; empty ranges yield no
	// entries. The chunk slice goes through `Column::slice`, so encoding-specific
	// slice paths stay live - canonicalization only happens at the projection
	// boundary in the caller.
	pub fn iter_range_chunks(&self, start: usize, end: usize) -> Vec<(usize, usize, usize)> {
		debug_assert!(start <= end, "iter_range_chunks: start {start} > end {end}");
		let mut out = Vec::new();
		if start == end {
			return out;
		}
		let mut chunk_offset = 0usize;
		for (idx, chunk) in self.chunks.iter().enumerate() {
			let chunk_len = chunk.len();
			let chunk_end = chunk_offset + chunk_len;
			if chunk_offset >= end {
				break;
			}
			if chunk_end > start && chunk_len > 0 {
				let intra_start = start.saturating_sub(chunk_offset);
				let intra_end = (end - chunk_offset).min(chunk_len);
				out.push((idx, intra_start, intra_end));
			}
			chunk_offset = chunk_end;
		}
		out
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

	// Build a lightweight view of rows `[start, end)` by slicing each column's
	// chunks via `Column::slice`. Schema is shared (Arc-bumped). For canonical
	// encodings the slice is an Arc-bump on the underlying buffer; compressed
	// encodings retain their compressed form, which is what makes batch-scoped
	// predicate eval cheap. Used by `SnapshotReader` to evaluate a predicate
	// against just the rows of the current batch.
	pub fn view_range(&self, start: usize, end: usize) -> Result<ColumnBlock> {
		debug_assert!(start <= end, "view_range: start {start} > end {end}");
		let mut sliced_columns = Vec::with_capacity(self.columns.len());
		for column in &self.columns {
			let ranges = column.iter_range_chunks(start, end);
			let mut sliced_chunks = Vec::with_capacity(ranges.len());
			for (idx, s, e) in ranges {
				sliced_chunks.push(column.chunks[idx].slice(s, e)?);
			}
			sliced_columns.push(ColumnChunks::new(column.ty.clone(), column.nullable, sliced_chunks));
		}
		Ok(ColumnBlock::new(Arc::clone(&self.schema), sliced_columns))
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

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{array::canonical::Canonical, buffer::ColumnBuffer};

	use super::*;

	fn chunked_int4(parts: &[&[i32]]) -> ColumnChunks {
		let chunks = parts
			.iter()
			.map(|p| {
				Column::from_canonical(
					Canonical::from_column_buffer(&ColumnBuffer::int4(p.to_vec())).unwrap(),
				)
			})
			.collect();
		ColumnChunks::new(Type::Int4, false, chunks)
	}

	#[test]
	fn iter_range_chunks_single_chunk_covers_range() {
		let ch = chunked_int4(&[&[1, 2, 3, 4, 5]]);
		assert_eq!(ch.iter_range_chunks(1, 4), vec![(0, 1, 4)]);
	}

	#[test]
	fn iter_range_chunks_spans_two_chunks() {
		let ch = chunked_int4(&[&[1, 2, 3], &[4, 5, 6]]);
		assert_eq!(ch.iter_range_chunks(2, 5), vec![(0, 2, 3), (1, 0, 2)]);
	}

	#[test]
	fn iter_range_chunks_skips_chunks_outside_range() {
		let ch = chunked_int4(&[&[1, 2], &[3, 4], &[5, 6]]);
		assert_eq!(ch.iter_range_chunks(2, 4), vec![(1, 0, 2)]);
	}

	#[test]
	fn iter_range_chunks_empty_range_yields_nothing() {
		let ch = chunked_int4(&[&[1, 2, 3]]);
		assert!(ch.iter_range_chunks(2, 2).is_empty());
	}

	#[test]
	fn iter_range_chunks_full_block_walks_every_chunk() {
		let ch = chunked_int4(&[&[1, 2], &[3, 4, 5], &[6]]);
		assert_eq!(ch.iter_range_chunks(0, 6), vec![(0, 0, 2), (1, 0, 3), (2, 0, 1)]);
	}

	fn block_with_columns(named: &[(&str, &[&[i32]])]) -> ColumnBlock {
		let schema: Schema =
			Arc::new(named.iter().map(|(n, _)| ((*n).to_string(), Type::Int4, false)).collect());
		let cols = named.iter().map(|(_, parts)| chunked_int4(parts)).collect();
		ColumnBlock::new(schema, cols)
	}

	#[test]
	fn view_range_empty_window_yields_empty_per_column() {
		let block = block_with_columns(&[("a", &[&[1, 2, 3]])]);
		let view = block.view_range(2, 2).unwrap();
		assert_eq!(view.len(), 0);
		assert_eq!(view.columns[0].chunks.len(), 0);
	}

	#[test]
	fn view_range_spanning_chunks_preserves_total_length() {
		let block = block_with_columns(&[("a", &[&[10, 20, 30], &[40, 50], &[60, 70, 80, 90]])]);
		let view = block.view_range(2, 7).unwrap();
		assert_eq!(view.len(), 5);
		assert_eq!(view.schema.len(), 1);
		// Three chunks contribute: chunk0[2..3] + chunk1[0..2] + chunk2[0..2].
		assert_eq!(view.columns[0].chunks.len(), 3);
		let vals: Vec<String> =
			(0..view.columns[0].len()).map(|i| view.columns[0].chunks_value_at(i).to_string()).collect();
		assert_eq!(vals, vec!["30", "40", "50", "60", "70"]);
	}

	#[test]
	fn view_range_multi_column_aligns_per_column_lengths() {
		let block = block_with_columns(&[("a", &[&[1, 2, 3, 4, 5]]), ("b", &[&[10, 20], &[30, 40, 50]])]);
		let view = block.view_range(1, 4).unwrap();
		assert_eq!(view.len(), 3);
		assert_eq!(view.columns[0].len(), 3);
		assert_eq!(view.columns[1].len(), 3);
	}

	impl ColumnChunks {
		fn chunks_value_at(&self, mut idx: usize) -> Value {
			for chunk in &self.chunks {
				if idx < chunk.len() {
					return chunk.get_value(idx);
				}
				idx -= chunk.len();
			}
			panic!("out of range");
		}
	}
}
