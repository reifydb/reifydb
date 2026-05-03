// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc, time::Duration};

use reifydb_core::event::EventBus;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::event::metric::MultiCommittedEvent;
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSystem},
	context::clock::Clock,
	pool::{PoolConfig, Pools},
	sync::waiter::WaiterHandle,
};
use tracing::instrument;

use crate::{
	BufferConfig, buffer::storage::BufferStorage, config::MultiStoreConfig, flush::actor::FlushMessage,
	persistent::PersistentStorage,
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{
	config::PersistentConfig,
	flush::{actor::FlushActor, listener::FlushEventListener},
};

pub mod drop;
pub mod multi;
pub mod router;
pub mod version;
pub mod worker;

use reifydb_core::actors::drop::DropMessage;
use worker::{DropActor, DropWorkerConfig};

use crate::Result;

#[derive(Clone)]
pub struct StandardMultiStore(Arc<StandardMultiStoreInner>);

pub struct StandardMultiStoreInner {
	pub(crate) buffer: Option<BufferStorage>,
	pub(crate) persistent: Option<PersistentStorage>,

	pub(crate) drop_actor: ActorRef<DropMessage>,

	#[allow(dead_code)]
	pub(crate) flush_actor: Option<ActorRef<FlushMessage>>,

	_actor_system: ActorSystem,

	pub(crate) event_bus: EventBus,
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::new", level = "debug", skip(config), fields(
		has_buffer = config.buffer.is_some(),
		has_persistent = config.persistent.is_some(),
	))]
	pub fn new(config: MultiStoreConfig) -> Result<Self> {
		let buffer = config.buffer.map(|c| c.storage);

		let actor_system = config.actor_system.clone();

		let storage = buffer.as_ref().expect("buffer tier is required");
		let drop_config = DropWorkerConfig::default();
		let drop_actor = DropActor::spawn(
			&actor_system,
			drop_config,
			storage.clone(),
			config.event_bus.clone(),
			config.clock,
		);

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush_actor) = {
			let persistent_config = config.persistent.clone();
			let persistent = persistent_config.as_ref().map(|c| c.storage.clone());
			let flush_actor = match (persistent.as_ref(), persistent_config.as_ref()) {
				(Some(persistent_storage), Some(persistent_cfg)) => {
					let actor_ref = FlushActor::spawn(
						&actor_system,
						storage.clone(),
						persistent_storage.clone(),
						persistent_cfg.flush_interval,
					);
					config.event_bus.register::<MultiCommittedEvent, _>(FlushEventListener::new(
						actor_ref.clone(),
					));
					Some(actor_ref)
				}
				_ => None,
			};
			(persistent, flush_actor)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush_actor): (Option<PersistentStorage>, Option<ActorRef<FlushMessage>>) = {
			let _ = config.persistent;
			(None, None)
		};

		Ok(Self(Arc::new(StandardMultiStoreInner {
			buffer,
			persistent,
			drop_actor,
			flush_actor,
			_actor_system: actor_system,
			event_bus: config.event_bus,
		})))
	}

	pub fn buffer(&self) -> Option<&BufferStorage> {
		self.buffer.as_ref()
	}

	pub fn persistent(&self) -> Option<&PersistentStorage> {
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
}

impl Deref for StandardMultiStore {
	type Target = StandardMultiStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl StandardMultiStore {
	pub fn testing_memory() -> Self {
		let pools = Pools::new(PoolConfig::sync_only());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::testing_memory_with_eventbus(EventBus::new(&actor_system))
	}

	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		let pools = Pools::new(PoolConfig::sync_only());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::new(MultiStoreConfig {
			buffer: Some(BufferConfig {
				storage: BufferStorage::memory(),
			}),
			persistent: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap()
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::testing_memory_with_persistent_sqlite_with_eventbus(EventBus::new(&actor_system))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite_with_eventbus(event_bus: EventBus) -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::new(MultiStoreConfig {
			buffer: Some(BufferConfig {
				storage: BufferStorage::memory(),
			}),
			persistent: Some(PersistentConfig::sqlite_in_memory()),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap()
	}
}
