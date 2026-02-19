// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::{
	event::EventBus,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_type::Result;

pub mod cold;
pub mod hot;
pub mod tier;
pub mod warm;

pub mod config;
pub mod multi;
pub mod store;

use config::{HotConfig, MultiStoreConfig};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::store::{
		MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionGetPrevious, MultiVersionStore,
		MultiVersionValues,
	},
};
use reifydb_type::util::cowvec::CowVec;
use store::StandardMultiStore;

pub mod memory {}
pub mod sqlite {}

pub struct MultiStoreVersion;

impl HasVersion for MultiStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
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

	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		MultiStore::Standard(StandardMultiStore::testing_memory_with_eventbus(event_bus))
	}

	/// Get access to the hot storage tier.
	///
	/// Returns `None` if the hot tier is not configured.
	pub fn hot(&self) -> Option<&hot::storage::HotStorage> {
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

impl MultiVersionGetPrevious for MultiStore {
	#[inline]
	fn get_previous_version(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> Result<Option<MultiVersionValues>> {
		match self {
			MultiStore::Standard(store) => store.get_previous_version(key, before_version),
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
