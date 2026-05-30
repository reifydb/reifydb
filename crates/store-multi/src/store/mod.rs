// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	mem,
	ops::Deref,
	sync::{Arc, OnceLock},
	time::Duration,
};

use reifydb_core::{encoded::key::EncodedKey, event::EventBus};
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSystem},
	context::clock::Clock,
	pool::{PoolConfig, Pools},
	shutdown::Shutdown,
	sync::{rwlock::RwLock, waiter::WaiterHandle},
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteTempPathGuard;
use tracing::instrument;

use crate::{
	CommitBufferConfig,
	config::MultiStoreConfig,
	flush::{ShapePersistence, actor::FlushMessage},
	gc::EvictionWatermark,
	tier::{
		commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier,
		read::buffer::MultiReadBufferTier,
	},
};

pub const DEFAULT_READ_BUFFER_CAPACITY: usize = 4096;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{config::PersistentConfig, flush::actor::FlushActor};

pub mod drop;
pub mod multi;
pub mod router;
pub mod worker;

use reifydb_core::actors::drop::DropMessage;
use worker::{DropActor, DropWorkerConfig};

use crate::Result;

#[derive(Clone)]
pub struct StandardMultiStore(Arc<StandardMultiStoreInner>);

pub struct StandardMultiStoreInner {
	pub(crate) commit: Option<MultiCommitBufferTier>,
	pub(crate) persistent: Option<MultiPersistentTier>,
	pub(crate) read: Option<MultiReadBufferTier>,
	pub(crate) drop_actor: Option<ActorRef<DropMessage>>,

	#[allow(dead_code)]
	pub(crate) flush_actor: Option<ActorRef<FlushMessage>>,
	#[allow(dead_code)]
	pub(crate) row_settings_provider: Arc<OnceLock<Arc<dyn ShapePersistence>>>,
	#[allow(dead_code)]
	pub(crate) eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,

	pub(crate) event_bus: EventBus,
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::new", level = "debug", skip(config), fields(
		has_commit = config.commit.is_some(),
		has_persistent = config.persistent.is_some(),
	))]
	pub fn new(config: MultiStoreConfig) -> Result<Self> {
		let commit = config.commit.map(|c| c.storage);

		let spawner = config.spawner.clone();

		let row_settings_provider: Arc<OnceLock<Arc<dyn ShapePersistence>>> = Arc::new(OnceLock::new());

		let eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));

		let drop_actor = commit.as_ref().map(|storage| {
			let drop_config = DropWorkerConfig::default();
			DropActor::spawn(&spawner, drop_config, storage.clone(), config.event_bus.clone(), config.clock)
		});

		let read = match (commit.as_ref(), config.persistent.is_some()) {
			(Some(_), true) => Some(MultiReadBufferTier::new(DEFAULT_READ_BUFFER_CAPACITY)),
			_ => None,
		};

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush_actor) = {
			let persistent_config = config.persistent.clone();
			let persistent = persistent_config.as_ref().map(|c| c.storage.clone());
			let flush_actor = match (commit.as_ref(), persistent.as_ref(), persistent_config.as_ref()) {
				(Some(buf), Some(persistent_storage), Some(persistent_cfg)) => {
					let actor_ref = FlushActor::spawn(
						&spawner,
						buf.clone(),
						persistent_storage.clone(),
						persistent_cfg.flush_interval,
						row_settings_provider.clone(),
						eviction_watermark.clone(),
						read.clone(),
					);
					Some(actor_ref)
				}
				_ => None,
			};
			(persistent, flush_actor)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush_actor): (Option<MultiPersistentTier>, Option<ActorRef<FlushMessage>>) = {
			let _ = config.persistent;
			(None, None)
		};

		let read = persistent.as_ref().and(read);

		Ok(Self(Arc::new(StandardMultiStoreInner {
			commit,
			persistent,
			read,
			drop_actor,
			flush_actor,
			row_settings_provider,
			eviction_watermark,
			event_bus: config.event_bus,
		})))
	}

	pub fn configure_read_buffer_capacity(&self, capacity: usize) {
		if let Some(read) = &self.read {
			read.set_capacity(capacity);
		}
	}

	pub fn invalidate_read_key(&self, key: &EncodedKey) {
		if let Some(read) = &self.read {
			read.invalidate(key);
		}
	}

	pub fn clear_read(&self) {
		if let Some(read) = &self.read {
			read.clear();
		}
	}

	pub fn set_row_settings_provider(&self, provider: Arc<dyn ShapePersistence>) {
		let _ = self.row_settings_provider.set(provider);
	}

	pub fn set_eviction_watermark(&self, watermark: Arc<dyn EvictionWatermark>) {
		*self.eviction_watermark.write() = Some(watermark);
	}

	pub fn clear_eviction_watermark(&self) {
		*self.eviction_watermark.write() = None;
	}

	pub fn commit(&self) -> Option<&MultiCommitBufferTier> {
		self.commit.as_ref()
	}

	pub fn persistent(&self) -> Option<&MultiPersistentTier> {
		self.persistent.as_ref()
	}

	pub fn flush_pending_blocking(&self) {
		let Some(actor_ref) = self.flush_actor.as_ref() else {
			return;
		};

		self.event_bus.wait_for_completion();

		let waiter = Arc::new(WaiterHandle::new());
		let waiter_for_msg = Arc::clone(&waiter);
		if actor_ref
			.send_blocking(FlushMessage::FlushPending {
				waiter: waiter_for_msg,
			})
			.is_err()
		{
			return;
		}

		waiter.wait_timeout(Duration::from_secs(60));
	}

	pub fn flush_all_blocking(&self) {
		let Some(actor_ref) = self.flush_actor.as_ref() else {
			return;
		};

		self.event_bus.wait_for_completion();

		let waiter = Arc::new(WaiterHandle::new());
		let waiter_for_msg = Arc::clone(&waiter);
		if actor_ref
			.send_blocking(FlushMessage::FlushAll {
				waiter: waiter_for_msg,
			})
			.is_err()
		{
			return;
		}

		waiter.wait_timeout(Duration::from_secs(60));
	}
}

impl Deref for StandardMultiStore {
	type Target = StandardMultiStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Shutdown for StandardMultiStore {
	fn shutdown(&self) {
		if let Some(persistent) = self.persistent.as_ref() {
			persistent.shutdown();
		}
	}
}

impl StandardMultiStore {
	pub fn testing_memory() -> Self {
		let pools = Pools::new(PoolConfig::sync_only());
		let clock = Clock::testing();
		let actor_system = ActorSystem::new(pools, clock.clone());
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		mem::forget(actor_system);
		Self::new(MultiStoreConfig {
			commit: Some(CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			}),
			persistent: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap()
	}

	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		let pools = Pools::new(PoolConfig::sync_only());
		let clock = Clock::testing();
		let actor_system = ActorSystem::new(pools, clock.clone());
		let spawner = actor_system.spawner();
		mem::forget(actor_system);
		Self::new(MultiStoreConfig {
			commit: Some(CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			}),
			persistent: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap()
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> (Self, SqliteTempPathGuard) {
		let pools = Pools::new(PoolConfig::default());
		let clock = Clock::testing();
		let actor_system = ActorSystem::new(pools, clock.clone());
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		mem::forget(actor_system);
		let (persistent, guard) = PersistentConfig::sqlite_in_memory();
		let store = Self::new(MultiStoreConfig {
			commit: Some(CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			}),
			persistent: Some(persistent),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap();
		(store, guard)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite_with_eventbus(event_bus: EventBus) -> (Self, SqliteTempPathGuard) {
		let pools = Pools::new(PoolConfig::default());
		let clock = Clock::testing();
		let actor_system = ActorSystem::new(pools, clock.clone());
		let spawner = actor_system.spawner();
		mem::forget(actor_system);
		let (persistent, guard) = PersistentConfig::sqlite_in_memory();
		let store = Self::new(MultiStoreConfig {
			commit: Some(CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			}),
			persistent: Some(persistent),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap();
		(store, guard)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_persistent_sqlite_only() -> (Self, SqliteTempPathGuard) {
		let pools = Pools::new(PoolConfig::default());
		let clock = Clock::testing();
		let actor_system = ActorSystem::new(pools, clock.clone());
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		mem::forget(actor_system);
		let (persistent, guard) = PersistentConfig::sqlite_in_memory();
		let store =
			Self::new(MultiStoreConfig::sqlite_unbuffered(persistent, spawner, clock, event_bus)).unwrap();
		(store, guard)
	}
}
