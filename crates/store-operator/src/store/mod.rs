// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod anchor;
mod census;
mod checkpoint;
mod state;

use std::{ops::Deref, sync::Arc};

use reifydb_core::{common::CommitVersion, lifecycle::watermark::CheckpointFloor, metrics::collect::MetricsCollector};
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorSpawner, ActorSystem},
	},
	context::clock::Clock,
	shutdown::Shutdown,
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{config::OperatorPersistentConfig, flush::OperatorFlushActor, sqlite::SqliteOperatorStorage};
use crate::{
	config::OperatorStoreConfig,
	flush::{FlushMessage, flush_now, flush_pending},
	tier::{
		commit::OperatorCommitBuffer,
		persistent::OperatorPersistentTier,
		read::{OperatorReadBufferShardMetrics, OperatorReadBufferTier},
	},
};

#[repr(u8)]
#[derive(Clone)]
pub enum OperatorStore {
	Standard(StandardOperatorStore) = 0,
}

#[derive(Clone)]
pub struct StandardOperatorStore(Arc<StandardOperatorStoreInner>);

pub struct StandardOperatorStoreInner {
	pub(crate) commit: OperatorCommitBuffer,
	pub(crate) persistent: Option<OperatorPersistentTier>,
	pub(crate) read: Option<OperatorReadBufferTier>,
	pub(crate) flush: Option<ActorRef<FlushMessage>>,
	#[allow(dead_code)]
	pub(crate) spawner: ActorSpawner,
}

impl Deref for StandardOperatorStore {
	type Target = StandardOperatorStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl StandardOperatorStore {
	pub fn new(config: OperatorStoreConfig) -> Self {
		let commit = config.commit.storage;
		let spawner = config.spawner;
		let read = config
			.persistent
			.is_some()
			.then(|| config.read.and_then(OperatorReadBufferTier::new))
			.flatten();

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush) = {
			let flush = config.persistent.as_ref().map(|persistent| {
				OperatorFlushActor::spawn(
					&spawner,
					commit.clone(),
					persistent.storage.clone(),
					read.clone(),
					persistent.flush_interval,
				)
			});
			(config.persistent.map(|persistent| persistent.storage), flush)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush): (Option<OperatorPersistentTier>, Option<ActorRef<FlushMessage>>) = match config.persistent {
			Some(persistent) => match persistent.storage {},
			None => (None, None),
		};

		let read = persistent.as_ref().and(read);

		Self(Arc::new(StandardOperatorStoreInner {
			commit,
			persistent,
			read,
			flush,
			spawner,
		}))
	}

	pub fn commit(&self) -> &OperatorCommitBuffer {
		&self.commit
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn persistent(&self) -> Option<&SqliteOperatorStorage> {
		self.persistent.as_ref().map(OperatorPersistentTier::sqlite_storage)
	}

	pub fn flush_pending_blocking(&self) -> bool {
		match &self.flush {
			Some(actor) => flush_pending(actor),
			None => true,
		}
	}

	pub fn read(&self) -> Option<&OperatorReadBufferTier> {
		self.read.as_ref()
	}

	pub fn read_buffer_shard_metrics(&self) -> Vec<OperatorReadBufferShardMetrics> {
		self.read.as_ref().map(OperatorReadBufferTier::shard_metrics).unwrap_or_default()
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		let mut collectors =
			self.persistent.as_ref().map(OperatorPersistentTier::metrics_collectors).unwrap_or_default();
		if let Some(read) = &self.read {
			collectors.push(Arc::new(read.clone()));
		}
		collectors
	}
}

impl Shutdown for StandardOperatorStore {
	fn shutdown(&self) {
		let Some(persistent) = self.persistent.as_ref() else {
			return;
		};
		flush_now(&self.commit, persistent, self.read.as_ref());
		persistent.shutdown();
	}
}

impl OperatorStore {
	pub fn standard(config: OperatorStoreConfig) -> Self {
		Self::Standard(StandardOperatorStore::new(config))
	}

	pub fn testing_memory() -> Self {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		Self::standard(OperatorStoreConfig::memory(spawner, clock))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> (Self, SqliteTempPathGuard) {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		let (persistent, guard) = OperatorPersistentConfig::sqlite_in_memory();
		(Self::standard(OperatorStoreConfig::sqlite(persistent, spawner, clock)), guard)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self::standard(OperatorStoreConfig::sqlite(OperatorPersistentConfig::sqlite(config), spawner, clock))
	}

	pub fn commit(&self) -> &OperatorCommitBuffer {
		match self {
			Self::Standard(store) => store.commit(),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn persistent(&self) -> Option<&SqliteOperatorStorage> {
		match self {
			Self::Standard(store) => store.persistent(),
		}
	}

	pub fn flush_pending_blocking(&self) -> bool {
		match self {
			Self::Standard(store) => store.flush_pending_blocking(),
		}
	}

	pub fn read(&self) -> Option<&OperatorReadBufferTier> {
		match self {
			Self::Standard(store) => store.read(),
		}
	}

	pub fn read_buffer_shard_metrics(&self) -> Vec<OperatorReadBufferShardMetrics> {
		match self {
			Self::Standard(store) => store.read_buffer_shard_metrics(),
		}
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		match self {
			Self::Standard(store) => store.metrics_collectors(),
		}
	}
}

impl CheckpointFloor for OperatorStore {
	fn floor(&self) -> Option<CommitVersion> {
		self.checkpoint_floor()
	}
}

impl Shutdown for OperatorStore {
	fn shutdown(&self) {
		match self {
			Self::Standard(store) => store.shutdown(),
		}
	}
}
