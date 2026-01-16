// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::{
	event::EventBus,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_type::Result;

pub mod config;
pub mod hot;
pub mod store;
pub mod tier;

use config::{HotConfig, SingleStoreConfig};
use reifydb_core::{
	delta::Delta,
	interface::store::{
		SingleVersionBatch, SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
		SingleVersionRangeRev, SingleVersionRemove, SingleVersionSet, SingleVersionStore, SingleVersionValues,
	},
	value::encoded::key::{EncodedKey, EncodedKeyRange},
};
use reifydb_type::util::cowvec::CowVec;
use store::StandardSingleStore;

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

	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		SingleStore::Standard(StandardSingleStore::testing_memory_with_eventbus(event_bus))
	}

	/// Get access to the hot storage tier.
	///
	/// Returns `None` if the hot tier is not configured.
	pub fn hot(&self) -> Option<&hot::tier::HotTier> {
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
