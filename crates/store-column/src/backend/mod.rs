// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod memory;

use std::sync::Arc;

pub use memory::MemoryColumnBackend;
use reifydb_core::{
	CommitVersion,
	interface::ColumnStatistics,
	value::column::{ColumnData, CompressedColumn},
};
use reifydb_type::Result;

#[repr(u8)]
#[derive(Clone)]
pub enum Backend {
	Memory(MemoryColumnBackend) = 0,
	Custom(Arc<dyn ColumnBackend>) = 254, // High discriminant for future built-in backends
}

impl Backend {
	#[inline]
	pub fn insert(&self, version: CommitVersion, columns: Vec<CompressedColumn>) -> Result<()> {
		match self {
			Backend::Memory(m) => m.insert(version, columns),
			Backend::Custom(c) => c.insert(version, columns),
		}
	}

	#[inline]
	pub fn scan(&self, version: CommitVersion, column_indices: &[usize]) -> Result<Vec<ColumnData>> {
		match self {
			Backend::Memory(m) => m.scan(version, column_indices),
			Backend::Custom(c) => c.scan(version, column_indices),
		}
	}

	#[inline]
	pub fn statistics(&self, column_index: usize) -> Option<ColumnStatistics> {
		match self {
			Backend::Memory(m) => m.statistics(column_index),
			Backend::Custom(c) => c.statistics(column_index),
		}
	}

	#[inline]
	pub fn partition_count(&self) -> usize {
		match self {
			Backend::Memory(m) => m.partition_count(),
			Backend::Custom(c) => c.partition_count(),
		}
	}

	#[inline]
	pub fn compressed_size(&self) -> usize {
		match self {
			Backend::Memory(m) => m.compressed_size(),
			Backend::Custom(c) => c.compressed_size(),
		}
	}

	#[inline]
	pub fn uncompressed_size(&self) -> usize {
		match self {
			Backend::Memory(m) => m.uncompressed_size(),
			Backend::Custom(c) => c.uncompressed_size(),
		}
	}

	#[inline]
	pub fn name(&self) -> &str {
		match self {
			Backend::Memory(m) => m.name(),
			Backend::Custom(c) => c.name(),
		}
	}

	#[inline]
	pub fn is_available(&self) -> bool {
		match self {
			Backend::Memory(m) => m.is_available(),
			Backend::Custom(c) => c.is_available(),
		}
	}
}

/// Trait for custom column storage backends
pub trait ColumnBackend: Send + Sync + 'static {
	fn insert(&self, version: CommitVersion, columns: Vec<CompressedColumn>) -> Result<()>;
	fn scan(&self, version: CommitVersion, column_indices: &[usize]) -> Result<Vec<ColumnData>>;
	fn statistics(&self, column_index: usize) -> Option<ColumnStatistics>;
	fn partition_count(&self) -> usize;
	fn compressed_size(&self) -> usize;
	fn uncompressed_size(&self) -> usize;

	// Metadata methods
	fn name(&self) -> &str;
	fn is_available(&self) -> bool;
}
