// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Primitive storage traits for raw key-value operations.
//!
//! This module defines the minimal interface that storage backends must implement.
//! All MVCC, CDC, and routing logic belongs in the store layer above.

use std::{collections::HashMap, ops::Bound};

use async_trait::async_trait;
use reifydb_core::interface::{FlowNodeId, PrimitiveId};
use reifydb_type::Result;

/// Identifies a logical table/namespace in storage.
///
/// The store layer routes keys to the appropriate table based on key type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableId {
	/// Multi-version storage for general data
	Multi,
	/// Single-version storage (no version history)
	Single,
	/// Change Data Capture entries (keyed by version)
	Cdc,
	/// Per-source table for row data
	Source(PrimitiveId),
	/// Per-operator table for flow node state
	Operator(FlowNodeId),
}

/// A raw storage entry.
///
/// Value is None for tombstones (deletions).
#[derive(Debug, Clone)]
pub struct RawEntry {
	pub key: Vec<u8>,
	pub value: Option<Vec<u8>>,
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

/// The primitive key-value storage trait.
///
/// This is intentionally minimal - just raw bytes in/out.
/// All MVCC, CDC, and routing logic belongs in the store layer above.
///
/// Implementations must be thread-safe and cloneable.
#[async_trait]
pub trait PrimitiveStorage: Send + Sync + Clone + 'static {
	/// Get the value for a key, or None if not found.
	async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>>;

	/// Check if a key exists in storage.
	async fn contains(&self, table: TableId, key: &[u8]) -> Result<bool> {
		Ok(self.get(table, key).await?.is_some())
	}

	/// Write entries to multiple tables atomically.
	///
	/// All entries across all tables are written in a single transaction.
	/// This ensures durability and atomicity for multi-table commits.
	async fn set(&self, batches: HashMap<TableId, Vec<(Vec<u8>, Option<Vec<u8>>)>>) -> Result<()>;

	/// Fetch a batch of entries in key order (ascending).
	///
	/// Returns up to `batch_size` entries. The `has_more` field indicates
	/// whether there are more entries after this batch. Use the last key
	/// from the batch as the `start` bound (excluded) for the next call.
	async fn range_batch(
		&self,
		table: TableId,
		start: Bound<Vec<u8>>,
		end: Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<RangeBatch>;

	/// Fetch a batch of entries in reverse key order (descending).
	///
	/// Returns up to `batch_size` entries. The `has_more` field indicates
	/// whether there are more entries after this batch. Use the last key
	/// from the batch as the `end` bound (excluded) for the next call.
	async fn range_rev_batch(
		&self,
		table: TableId,
		start: Bound<Vec<u8>>,
		end: Bound<Vec<u8>>,
		batch_size: usize,
	) -> Result<RangeBatch>;

	/// Ensure a table exists (creates if needed).
	///
	/// For memory backends this is typically a no-op.
	/// For SQL backends this may create tables.
	async fn ensure_table(&self, table: TableId) -> Result<()>;

	/// Delete all entries in a table.
	async fn clear_table(&self, table: TableId) -> Result<()>;
}

/// Marker trait for backends that support the primitive storage interface.
pub trait PrimitiveBackend: PrimitiveStorage {}
