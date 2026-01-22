// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Common storage tier traits and types.
//!
//! This module defines the minimal interface that all storage tiers (hot, warm, cold)
//! must implement. All MVCC, CDC, and routing logic belongs in the store layer above.

use std::{collections::HashMap, ops::Bound};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{flow::FlowNodeId, primitive::PrimitiveId},
};
use reifydb_type::{Result, util::cowvec::CowVec};

/// Identifies a logical table/namespace in storage.
///
/// The store layer routes keys to the appropriate storage based on key type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
	/// Multi-version storage for general data
	Multi,
	/// Per-source table for row data
	Source(PrimitiveId),
	/// Per-operator table for flow node state
	Operator(FlowNodeId),
}

/// A raw storage entry with version.
///
/// Value is None for tombstones (deletions).
#[derive(Debug, Clone)]
pub struct RawEntry {
	pub key: CowVec<u8>,
	pub version: CommitVersion,
	pub value: Option<CowVec<u8>>,
}

/// A batch of range results with continuation info for pagination.
#[derive(Debug, Clone)]
pub struct RangeBatch {
	/// The entries in this batch.
	pub entries: Vec<RawEntry>,
	/// Whether there are more entries after this batch.
	pub has_more: bool,
}

impl RangeBatch {
	/// Creates an empty batch with no more results.
	pub fn empty() -> Self {
		Self {
			entries: Vec::new(),
			has_more: false,
		}
	}

	/// Returns true if this batch contains no entries.
	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}
}

/// Cursor state for streaming range queries.
///
/// Tracks position within a range scan, enabling efficient continuation
/// across multiple batches without re-scanning from the beginning.
#[derive(Debug, Clone)]
pub struct RangeCursor {
	/// Last key seen in the previous batch (for Bound::Excluded continuation)
	pub last_key: Option<CowVec<u8>>,
	/// Whether this stream is exhausted
	pub exhausted: bool,
}

impl RangeCursor {
	/// Create a new cursor at the start of a range.
	pub fn new() -> Self {
		Self {
			last_key: None,
			exhausted: false,
		}
	}

	/// Check if the stream is exhausted.
	pub fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

impl Default for RangeCursor {
	fn default() -> Self {
		Self::new()
	}
}

/// The tier storage trait.
///
/// This is intentionally minimal - just raw bytes in/out.
/// Version is a first-class parameter for all operations.
/// All MVCC, CDC, and routing logic belongs in the store layer above.
///
/// Implementations must be thread-safe and cloneable.

pub trait TierStorage: Send + Sync + Clone + 'static {
	/// Get the value for a key at or before the given version.
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<Option<CowVec<u8>>>;

	/// Check if a key exists at or before the given version.
	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		Ok(self.get(table, key, version)?.is_some())
	}

	/// Write entries to multiple tables atomically at the given version.
	///
	/// All entries across all tables are written in a single transaction.
	/// This ensures durability and atomicity for multi-table commits.
	fn set(
		&self,
		version: CommitVersion,
		batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>>,
	) -> Result<()>;

	/// Fetch the next batch of entries in key order at or before version.
	///
	/// Uses the cursor to track position. On first call, cursor should be new.
	/// On subsequent calls, pass the same cursor to continue from where left off.
	/// Returns up to `batch_size` entries. The cursor is updated with the last
	/// key seen, and `exhausted` is set to true when no more entries remain.
	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch>;

	/// Fetch the next batch of entries in reverse key order at or before version.
	///
	/// Uses the cursor to track position. On first call, cursor should be new.
	/// On subsequent calls, pass the same cursor to continue from where left off.
	/// Returns up to `batch_size` entries. The cursor is updated with the last
	/// key seen, and `exhausted` is set to true when no more entries remain.
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch>;

	/// Ensure a table exists (creates if needed).
	///
	/// For memory backends this is typically a no-op.
	/// For SQL backends this may create tables.
	fn ensure_table(&self, table: EntryKind) -> Result<()>;

	/// Delete all entries in a table.
	fn clear_table(&self, table: EntryKind) -> Result<()>;

	/// Physically drop specific versions of entries from storage.
	///
	/// Unlike `set()` with None values which inserts tombstones for MVCC,
	/// this method actually removes entries from storage to reclaim memory.
	/// Used by the drop worker to erase old versions after they're no longer needed.
	///
	/// Each entry in the batch is a (key, version) pair identifying the specific
	/// version of the key to remove.
	fn drop(&self, batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()>;

	/// Get all versions of a specific key (for internal cleanup operations).
	///
	/// Unlike `get()` which does MVCC resolution, this returns ALL stored versions
	/// of the key with their values. Used by the drop worker to discover which
	/// versions exist before deciding which to clean up.
	///
	/// Returns a vector of (version, value) pairs, sorted by version descending.
	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>>;
}

/// Marker trait for storage tiers that support the tier storage interface.
pub trait TierBackend: TierStorage {}
