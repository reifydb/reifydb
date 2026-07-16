// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi-version storage backend for OLTP traffic. Implements the `MultiVersionStore` family of traits from
//! `core::interface::store` so the engine can read at a snapshot, write a new version, and step backwards through
//! history without coordinating with concurrent readers.
//!
//! The backend is tiered: hot writes land in the in-memory buffer, the flusher migrates them to persistent storage
//! at commit boundaries, and the garbage collector reclaims versions that have aged out behind the configured
//! retention. The persistent tier is pluggable - a SQLite-backed implementation is the default but the trait surface
//! is what the engine binds to, so other backends can be slotted in.
//!
//! Invariant: a row at `version V` is the value visible to a reader whose snapshot is `>= V` and where no later
//! version exists at `V' <= snapshot`. Commit must publish all deltas of a transaction atomically with respect to
//! readers; partial visibility breaks snapshot isolation.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::{
	event::EventBus,
	interface::version::{ComponentType, HasVersion, SystemVersion},
};
use reifydb_value::Result;

pub mod flush;
pub mod gc;
pub mod tier;

pub mod config;
pub mod store;

use std::{collections::HashMap, sync::Arc};

use config::{CommitBufferConfig, MultiStoreConfig};
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::flow::FlowNodeId,
		store::{
			MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionGetPrevious,
			MultiVersionRow, MultiVersionStore,
		},
	},
	util::memory::MemoryReporter,
};
use reifydb_runtime::shutdown::Shutdown;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};
use store::StandardMultiStore;
use tier::read::OperatorReadBufferUsage;

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

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> (Self, SqliteTempPathGuard) {
		let (store, guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		(MultiStore::Standard(store), guard)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite_with_eventbus(event_bus: EventBus) -> (Self, SqliteTempPathGuard) {
		let (store, guard) = StandardMultiStore::testing_memory_with_persistent_sqlite_with_eventbus(event_bus);
		(MultiStore::Standard(store), guard)
	}

	pub fn flush_pending_blocking(&self) {
		match self {
			MultiStore::Standard(store) => store.flush_pending_blocking(),
		}
	}

	pub fn flush_all_blocking(&self) {
		match self {
			MultiStore::Standard(store) => store.flush_all_blocking(),
		}
	}

	pub fn commit(&self) -> Option<&tier::commit::buffer::MultiCommitBufferTier> {
		match self {
			MultiStore::Standard(store) => store.commit(),
		}
	}

	pub fn memory_reporters(&self) -> Vec<Arc<dyn MemoryReporter>> {
		match self {
			MultiStore::Standard(store) => store.memory_reporters(),
		}
	}

	pub fn operator_read_buffer_usage(&self) -> Vec<OperatorReadBufferUsage> {
		match self {
			MultiStore::Standard(store) => store.operator_read_buffer_usage(),
		}
	}

	pub fn operator_disk_payload_bytes(&self) -> Vec<(FlowNodeId, ByteSize)> {
		match self {
			MultiStore::Standard(store) => store.operator_disk_payload_bytes(),
		}
	}

	pub fn persistent(&self) -> Option<&tier::persistent::MultiPersistentTier> {
		match self {
			MultiStore::Standard(store) => store.persistent(),
		}
	}

	pub fn clear_eviction_watermark(&self) {
		match self {
			MultiStore::Standard(store) => store.clear_eviction_watermark(),
		}
	}
}

impl Shutdown for MultiStore {
	fn shutdown(&self) {
		match self {
			MultiStore::Standard(store) => store.shutdown(),
		}
	}
}

impl MultiVersionGet for MultiStore {
	#[inline]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
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
			MultiStore::Standard(store) => MultiVersionCommit::commit(store, deltas, version),
		}
	}
}

impl MultiVersionGetPrevious for MultiStore {
	#[inline]
	fn get_previous_version(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> Result<Option<MultiVersionRow>> {
		match self {
			MultiStore::Standard(store) => store.get_previous_version(key, before_version),
		}
	}
}

pub type MultiVersionRangeIterator<'a> = Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a>;

/// Version scope for a multi-version range scan.
///
/// Selects which version is returned for each key during a range walk.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MultiVersionScope {
	/// For each key, yield the highest version `v` with `v <= read`.
	AsOf {
		read: CommitVersion,
	},
	/// For each key, yield the highest version `v` with `after < v <= read`.
	/// Keys with no qualifying version are dropped from the output.
	Between {
		after: CommitVersion,
		read: CommitVersion,
	},
}

impl MultiVersionScope {
	#[inline]
	pub fn read(&self) -> CommitVersion {
		match self {
			Self::AsOf {
				read,
			}
			| Self::Between {
				read,
				..
			} => *read,
		}
	}

	#[inline]
	pub fn contains(&self, v: CommitVersion) -> bool {
		match self {
			Self::AsOf {
				read,
			} => v <= *read,
			Self::Between {
				after,
				read,
			} => v > *after && v <= *read,
		}
	}
}

impl MultiStore {
	pub fn range(
		&self,
		range: EncodedKeyRange,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> MultiVersionRangeIterator<'_> {
		match self {
			MultiStore::Standard(store) => Box::new(store.range(range, scope, batch_size)),
		}
	}

	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> MultiVersionRangeIterator<'_> {
		match self {
			MultiStore::Standard(store) => Box::new(store.range_rev(range, scope, batch_size)),
		}
	}

	pub fn get_many(
		&self,
		keys: &[EncodedKey],
		version: CommitVersion,
	) -> Result<HashMap<EncodedKey, MultiVersionRow>> {
		match self {
			MultiStore::Standard(store) => store.get_many(keys, version),
		}
	}
}

impl MultiVersionStore for MultiStore {}
