// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_core::runtime::ComputePool;
pub use reifydb_type::Result;

pub mod cold;
pub mod hot;
pub mod tier;
pub mod warm;

pub(crate) mod cdc;
pub mod config;
mod multi;
// pub mod retention;
mod single;
pub mod stats;
mod store;

use std::collections::Bound;

pub use cdc::{CdcBatch, CdcCount, CdcGet, CdcRange, CdcStore};
pub use config::{
	ColdConfig, HotConfig, MergeConfig, RetentionConfig, StorageStatsConfig, TransactionStoreConfig, WarmConfig,
};
pub use multi::*;
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange,
	delta::Delta,
	interface::{Cdc, MultiVersionValues, SingleVersionValues},
};
pub use single::*;
pub use stats::{ObjectId, StorageStats, StorageTracker, Tier, TierStats};
pub use store::StandardTransactionStore;

pub mod memory {
	pub use crate::hot::memory::MemoryPrimitiveStorage;
}
pub mod sqlite {
	pub use crate::hot::sqlite::{SqliteConfig, SqlitePrimitiveStorage};
}

pub struct TransactionStoreVersion;

impl HasVersion for TransactionStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "store-transaction".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Transaction storage for OLTP operations and recent data".to_string(),
			r#type: ComponentType::Module,
		}
	}
}

#[repr(u8)]
#[derive(Clone)]
pub enum TransactionStore {
	Standard(StandardTransactionStore) = 0,
	// Other(Box<dyn >) = 254,
}

impl TransactionStore {
	pub fn standard(config: TransactionStoreConfig) -> Self {
		Self::Standard(StandardTransactionStore::new(config).unwrap())
	}
}

impl TransactionStore {
	pub fn testing_memory(compute_pool: ComputePool) -> Self {
		TransactionStore::Standard(StandardTransactionStore::testing_memory(compute_pool))
	}

	/// Get access to the storage tracker.
	pub fn stats_tracker(&self) -> &StorageTracker {
		match self {
			TransactionStore::Standard(store) => store.stats_tracker(),
		}
	}
}

// MultiVersion trait implementations

impl MultiVersionGet for TransactionStore {
	#[inline]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionValues>> {
		match self {
			TransactionStore::Standard(store) => MultiVersionGet::get(store, key, version),
		}
	}
}

impl MultiVersionContains for TransactionStore {
	#[inline]
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		match self {
			TransactionStore::Standard(store) => MultiVersionContains::contains(store, key, version),
		}
	}
}

impl MultiVersionCommit for TransactionStore {
	#[inline]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()> {
		match self {
			TransactionStore::Standard(store) => store.commit(deltas, version),
		}
	}
}

/// Iterator type for multi-version range results.
pub type MultiVersionRangeIterator<'a> = Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + 'a>;

impl TransactionStore {
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
			TransactionStore::Standard(store) => Box::new(store.range(range, version, batch_size)),
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
			TransactionStore::Standard(store) => Box::new(store.range_rev(range, version, batch_size)),
		}
	}
}

// SingleVersion trait implementations

impl SingleVersionGet for TransactionStore {
	#[inline]
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionValues>> {
		match self {
			TransactionStore::Standard(store) => SingleVersionGet::get(store, key),
		}
	}
}

impl SingleVersionContains for TransactionStore {
	#[inline]
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		match self {
			TransactionStore::Standard(store) => SingleVersionContains::contains(store, key),
		}
	}
}

impl SingleVersionSet for TransactionStore {}

impl SingleVersionRemove for TransactionStore {}

impl SingleVersionCommit for TransactionStore {
	#[inline]
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()> {
		match self {
			TransactionStore::Standard(store) => SingleVersionCommit::commit(store, deltas),
		}
	}
}

impl SingleVersionRange for TransactionStore {
	#[inline]
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		match self {
			TransactionStore::Standard(store) => SingleVersionRange::range_batch(store, range, batch_size),
		}
	}
}

impl SingleVersionRangeRev for TransactionStore {
	#[inline]
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		match self {
			TransactionStore::Standard(store) => {
				SingleVersionRangeRev::range_rev_batch(store, range, batch_size)
			}
		}
	}
}

// CDC trait implementations

impl CdcGet for TransactionStore {
	#[inline]
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
		match self {
			TransactionStore::Standard(store) => CdcGet::get(store, version),
		}
	}
}

impl CdcRange for TransactionStore {
	#[inline]
	fn range_batch(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> Result<CdcBatch> {
		match self {
			TransactionStore::Standard(store) => CdcRange::range_batch(store, start, end, batch_size),
		}
	}
}

impl CdcCount for TransactionStore {
	#[inline]
	fn count(&self, version: CommitVersion) -> Result<usize> {
		match self {
			TransactionStore::Standard(store) => CdcCount::count(store, version),
		}
	}
}

// High-level trait implementations
impl MultiVersionStore for TransactionStore {}
impl SingleVersionStore for TransactionStore {}
impl CdcStore for TransactionStore {}
