// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Single-version storage backend for workloads where snapshot isolation would only add overhead. Implements the
//! `SingleVersionStore` family of traits from `core::interface::store`: get, contains, set, remove, commit, and the
//! ranged scan iterators used by the engine for table and index walks.
//!
//! Writes are atomic per commit but never coexist with prior versions of the same key; readers always observe the
//! latest committed value. The buffered tier batches recent writes and the persistent tier owns durable state, the
//! same shape as the multi-version backend minus history.
//!
//! Invariant: a key's value after commit is the value the next reader sees - no version cursor, no time travel. Code
//! that needs history must use `store-multi`; reaching here for it returns nothing useful.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::{
	event::EventBus,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_type::Result;

pub mod buffer;
pub mod config;
pub mod store;
pub mod tier;

use config::{BufferConfig, SingleStoreConfig};
use reifydb_core::{
	delta::Delta,
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::store::{
		SingleVersionBatch, SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
		SingleVersionRangeRev, SingleVersionRemove, SingleVersionRow, SingleVersionSet, SingleVersionStore,
	},
};
use reifydb_type::util::cowvec::CowVec;
use store::StandardSingleStore;

pub struct SingleStoreVersion;

impl HasVersion for SingleStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
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

	pub fn buffer(&self) -> Option<&buffer::tier::BufferTier> {
		match self {
			SingleStore::Standard(store) => store.buffer(),
		}
	}
}

impl SingleVersionGet for SingleStore {
	#[inline]
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionRow>> {
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
