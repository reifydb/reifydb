// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Single-version storage backend for workloads where snapshot isolation would only add overhead: a
//! buffered tier over a persistent tier, the same shape as the multi-version backend minus history.
//! Invariant: a key's value after commit is what the next reader sees - no version cursor, no time travel.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use std::sync::Arc;

use reifydb_core::{
	interface::version::{ComponentType, HasVersion, SystemVersion},
	metrics::collect::MetricsCollector,
};
use reifydb_value::Result;

pub mod config;
pub mod flush;
pub mod store;
pub mod tier;

use config::SingleStoreConfig;
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	delta::Delta,
	interface::store::{
		SingleVersionBatch, SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
		SingleVersionRangeRev, SingleVersionRemove, SingleVersionRow, SingleVersionSet, SingleVersionStore,
	},
};
use reifydb_runtime::shutdown::Shutdown;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::util::cowvec::CowVec;
use store::StandardSingleStore;
use tier::{commit::buffer::SingleCommitMetrics, persistent::SinglePageCacheMetrics};

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

	pub fn commit(&self) -> Option<&tier::commit::buffer::SingleCommitBufferTier> {
		match self {
			SingleStore::Standard(store) => store.commit(),
		}
	}

	pub fn persistent(&self) -> Option<&tier::persistent::SinglePersistentTier> {
		match self {
			SingleStore::Standard(store) => store.persistent(),
		}
	}

	pub fn commit_metrics(&self) -> Option<SingleCommitMetrics> {
		match self {
			SingleStore::Standard(store) => store.commit_metrics(),
		}
	}

	pub fn persistent_page_cache_metrics(&self) -> Option<SinglePageCacheMetrics> {
		match self {
			SingleStore::Standard(store) => store.persistent_page_cache_metrics(),
		}
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		match self {
			SingleStore::Standard(store) => store.metrics_collectors(),
		}
	}

	pub fn flush_pending_blocking(&self) -> bool {
		match self {
			SingleStore::Standard(store) => store.flush_pending_blocking(),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> (Self, SqliteTempPathGuard) {
		let (store, guard) = StandardSingleStore::testing_memory_with_persistent_sqlite();
		(SingleStore::Standard(store), guard)
	}
}

impl Shutdown for SingleStore {
	fn shutdown(&self) {
		match self {
			SingleStore::Standard(store) => store.shutdown(),
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
