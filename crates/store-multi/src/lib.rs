// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
pub use reifydb_type::Result;

pub mod cold;
pub mod hot;
pub mod tier;
pub mod warm;

pub mod config;
mod multi;
mod store;

pub use config::{
	ColdConfig, HotConfig, MergeConfig, RetentionConfig, MultiStoreConfig, WarmConfig,
};
pub use multi::*;
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange,
	delta::Delta,
	interface::MultiVersionValues,
};
pub use store::{StandardMultiStore, StorageResolver};

pub mod memory {
	pub use crate::hot::memory::MemoryPrimitiveStorage;
}
pub mod sqlite {
	pub use crate::hot::sqlite::{SqliteConfig, SqlitePrimitiveStorage};
}

pub struct MultiStoreVersion;

impl HasVersion for MultiStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "store-multi".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Multi-version storage for OLTP operations with MVCC support".to_string(),
			r#type: ComponentType::Module,
		}
	}
}

#[repr(u8)]
#[derive(Clone)]
pub enum MultiStore {
	Standard(StandardMultiStore) = 0,
	// Other(Box<dyn MultiVersionStore>) = 254,
}

impl MultiStore {
	pub fn standard(config: MultiStoreConfig) -> Self {
		Self::Standard(StandardMultiStore::new(config).unwrap())
	}
}

impl MultiStore {
	pub fn testing_memory() -> Self {
		MultiStore::Standard(StandardMultiStore::testing_memory())
	}

	/// Get access to the hot storage tier.
	///
	/// Returns `None` if the hot tier is not configured.
	pub fn hot(&self) -> Option<&hot::HotStorage> {
		match self {
			MultiStore::Standard(store) => store.hot(),
		}
	}
}

// MultiVersion trait implementations

impl MultiVersionGet for MultiStore {
	#[inline]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionValues>> {
		match self {
			MultiStore::Standard(store) => MultiVersionGet::get(store, key, version),
		}
	}
}

impl MultiVersionContains for MultiStore {
	#[inline]
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		match self {
			MultiStore::Standard(store) => MultiVersionContains::contains(store, key, version),
		}
	}
}

impl MultiVersionCommit for MultiStore {
	#[inline]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()> {
		match self {
			MultiStore::Standard(store) => store.commit(deltas, version),
		}
	}
}

/// Iterator type for multi-version range results.
pub type MultiVersionRangeIterator<'a> = Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + 'a>;

impl MultiStore {
	/// Create an iterator for forward range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The iterator yields individual entries
	/// and maintains cursor state internally.
	pub fn range(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: usize,
	) -> MultiVersionRangeIterator<'_> {
		match self {
			MultiStore::Standard(store) => Box::new(store.range(range, version, batch_size)),
		}
	}

	/// Create an iterator for reverse range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The iterator yields individual entries
	/// in reverse key order and maintains cursor state internally.
	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: usize,
	) -> MultiVersionRangeIterator<'_> {
		match self {
			MultiStore::Standard(store) => Box::new(store.range_rev(range, version, batch_size)),
		}
	}
}

// High-level trait implementations
impl MultiVersionStore for MultiStore {}
