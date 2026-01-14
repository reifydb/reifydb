// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
pub use reifydb_type::Result;

pub mod hot;
pub mod tier;

pub mod config;
mod single;
mod store;

pub use config::{HotConfig, SingleStoreConfig};
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::SingleVersionValues};
pub use single::*;
pub use store::StandardSingleStore;

pub mod memory {
	pub use crate::hot::memory::MemoryPrimitiveStorage;
}
pub mod sqlite {
	pub use crate::hot::sqlite::{SqliteConfig, SqlitePrimitiveStorage};
}

pub struct SingleStoreVersion;

impl HasVersion for SingleStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "store-single".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Single-version storage for OLTP operations without version history".to_string(),
			r#type: ComponentType::Module,
		}
	}
}

#[repr(u8)]
#[derive(Clone)]
pub enum SingleStore {
	Standard(StandardSingleStore) = 0,
	// Other(Box<dyn SingleVersionStore>) = 254,
}

impl SingleStore {
	pub fn standard(config: SingleStoreConfig) -> Self {
		Self::Standard(StandardSingleStore::new(config).unwrap())
	}
}

impl SingleStore {
	pub fn testing_memory() -> Self {
		SingleStore::Standard(StandardSingleStore::testing_memory())
	}

	/// Get access to the hot storage tier.
	///
	/// Returns `None` if the hot tier is not configured.
	pub fn hot(&self) -> Option<&hot::HotTier> {
		match self {
			SingleStore::Standard(store) => store.hot(),
		}
	}
}

// SingleVersion trait implementations

impl SingleVersionGet for SingleStore {
	#[inline]
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionValues>> {
		match self {
			SingleStore::Standard(store) => SingleVersionGet::get(store, key),
		}
	}
}

impl SingleVersionContains for SingleStore {
	#[inline]
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		match self {
			SingleStore::Standard(store) => SingleVersionContains::contains(store, key),
		}
	}
}

impl SingleVersionSet for SingleStore {}

impl SingleVersionRemove for SingleStore {}

impl SingleVersionCommit for SingleStore {
	#[inline]
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()> {
		match self {
			SingleStore::Standard(store) => SingleVersionCommit::commit(store, deltas),
		}
	}
}

impl SingleVersionRange for SingleStore {
	#[inline]
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		match self {
			SingleStore::Standard(store) => SingleVersionRange::range_batch(store, range, batch_size),
		}
	}
}

impl SingleVersionRangeRev for SingleStore {
	#[inline]
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch> {
		match self {
			SingleStore::Standard(store) => {
				SingleVersionRangeRev::range_rev_batch(store, range, batch_size)
			}
		}
	}
}

impl SingleVersionStore for SingleStore {}
