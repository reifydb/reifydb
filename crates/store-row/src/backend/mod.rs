// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod commit;
pub(crate) mod diagnostic;
pub mod memory;
pub mod sqlite;

use std::sync::Arc;

// Re-export the backend wrappers from submodules
pub use memory::MemoryRowBackend;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::{MultiVersionValues, SingleVersionValues},
	value::encoded::EncodedValues,
};
pub use sqlite::SqliteRowBackend;

/// Backend enum with built-in variants and custom extension point
#[repr(u8)]
#[derive(Clone)]
pub enum Backend {
	Memory(MemoryRowBackend) = 0,
	Sqlite(SqliteRowBackend) = 1,
	Custom(Arc<dyn MultiVersionRowBackend>) = 254, // High discriminant for future built-in backends
}

impl Backend {
	#[inline]
	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>> {
		match self {
			Backend::Memory(m) => m.get(key, version), // Fast: direct call
			Backend::Sqlite(s) => s.get(key, version), // Fast: direct call
			Backend::Custom(c) => c.get(key, version), // Slow: dynamic dispatch
		}
	}

	#[inline]
	pub fn put(&self, row: MultiVersionValues) -> crate::Result<()> {
		match self {
			Backend::Memory(m) => m.put(row),
			Backend::Sqlite(s) => s.put(row),
			Backend::Custom(c) => c.put(row),
		}
	}

	#[inline]
	pub fn delete(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<()> {
		match self {
			Backend::Memory(m) => m.delete(key, version),
			Backend::Sqlite(s) => s.delete(key, version),
			Backend::Custom(c) => c.delete(key, version),
		}
	}

	#[inline]
	pub fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> crate::Result<Vec<MultiVersionValues>> {
		match self {
			Backend::Memory(m) => m.range(range, version),
			Backend::Sqlite(s) => s.range(range, version),
			Backend::Custom(c) => c.range(range, version),
		}
	}

	#[inline]
	pub fn count(&self) -> usize {
		match self {
			Backend::Memory(m) => m.count(),
			Backend::Sqlite(s) => s.count(),
			Backend::Custom(c) => c.count(),
		}
	}

	#[inline]
	pub fn name(&self) -> &str {
		match self {
			Backend::Memory(m) => m.name(),
			Backend::Sqlite(s) => s.name(),
			Backend::Custom(c) => c.name(),
		}
	}

	#[inline]
	pub fn is_available(&self) -> bool {
		match self {
			Backend::Memory(m) => m.is_available(),
			Backend::Sqlite(s) => s.is_available(),
			Backend::Custom(c) => c.is_available(),
		}
	}
}

/// Trait for custom storage backends
pub trait MultiVersionRowBackend: Send + Sync + 'static {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>>;
	fn put(&self, row: MultiVersionValues) -> crate::Result<()>;
	fn delete(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<()>;
	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> crate::Result<Vec<MultiVersionValues>>;
	fn count(&self) -> usize;

	// Metadata methods
	fn name(&self) -> &str;
	fn is_available(&self) -> bool;
}

// SingleVersion backend support (for completeness)
pub trait SingleVersionRowBackend: Send + Sync + 'static {
	fn get(&self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>>;
	fn put(&self, key: &EncodedKey, row: EncodedValues) -> crate::Result<()>;
	fn delete(&self, key: &EncodedKey) -> crate::Result<()>;
	fn scan(&self) -> crate::Result<Vec<SingleVersionValues>>;
}
