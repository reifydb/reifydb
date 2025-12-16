// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Primitive storage traits for raw key-value operations.
//!
//! This module defines the minimal interface that storage backends must implement.
//! All MVCC, CDC, and routing logic belongs in the store layer above.

use std::ops::Bound;

use reifydb_core::interface::{FlowNodeId, SourceId};
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
	Source(SourceId),
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

/// The primitive key-value storage trait.
///
/// This is intentionally minimal - just raw bytes in/out.
/// All MVCC, CDC, and routing logic belongs in the store layer above.
///
/// Implementations must be thread-safe and cloneable.
pub trait PrimitiveStorage: Send + Sync + Clone + 'static {
	/// Iterator type for forward range scans
	type RangeIter<'a>: Iterator<Item = Result<RawEntry>> + Send
	where
		Self: 'a;

	/// Iterator type for reverse range scans
	type RangeRevIter<'a>: Iterator<Item = Result<RawEntry>> + Send
	where
		Self: 'a;

	/// Get the value for a key, or None if not found.
	fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>>;

	/// Check if a key exists in storage.
	fn contains(&self, table: TableId, key: &[u8]) -> Result<bool> {
		Ok(self.get(table, key)?.is_some())
	}

	/// Store a batch of entries atomically.
	///
	/// Each entry is (key, optional_value). None value = tombstone/deletion.
	/// All entries go to the specified table.
	fn put(&self, table: TableId, entries: &[(&[u8], Option<&[u8]>)]) -> Result<()>;

	/// Iterate entries in key order (ascending).
	fn range(
		&self,
		table: TableId,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<Self::RangeIter<'_>>;

	/// Iterate entries in reverse key order (descending).
	fn range_rev(
		&self,
		table: TableId,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		batch_size: usize,
	) -> Result<Self::RangeRevIter<'_>>;

	/// Ensure a table exists (creates if needed).
	///
	/// For memory backends this is typically a no-op.
	/// For SQL backends this may create tables.
	fn ensure_table(&self, table: TableId) -> Result<()>;

	/// Delete all entries in a table.
	fn clear_table(&self, table: TableId) -> Result<()>;
}

/// Marker trait for backends that support the primitive storage interface.
pub trait PrimitiveBackend: PrimitiveStorage {}
