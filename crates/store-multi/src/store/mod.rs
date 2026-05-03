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
	HotConfig, cold::ColdStorage, config::MultiStoreConfig, flush::actor::FlushMessage, hot::storage::HotStorage,
	warm::WarmStorage,
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{
	config::WarmConfig,
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
	pub(crate) hot: Option<HotStorage>,
	pub(crate) warm: Option<WarmStorage>,
	pub(crate) cold: Option<ColdStorage>,

	pub(crate) drop_actor: ActorRef<DropMessage>,

	#[allow(dead_code)]
	pub(crate) flush_actor: Option<ActorRef<FlushMessage>>,

	_actor_system: ActorSystem,

	pub(crate) event_bus: EventBus,
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::new", level = "debug", skip(config), fields(
		has_hot = config.hot.is_some(),
		has_warm = config.warm.is_some(),
		has_cold = config.cold.is_some(),
	))]
	pub fn new(config: MultiStoreConfig) -> Result<Self> {
		let hot = config.hot.map(|c| c.storage);
		// TODO: cold is still a placeholder.
		let cold = None;
		let _ = config.cold;

		let actor_system = config.actor_system.clone();

		let storage = hot.as_ref().expect("hot tier is required");
		let drop_config = DropWorkerConfig::default();
		let drop_actor = DropActor::spawn(
			&actor_system,
			drop_config,
			storage.clone(),
			config.event_bus.clone(),
			config.clock,
		);

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (warm, flush_actor) = {
			let warm_config = config.warm.clone();
			let warm = warm_config.as_ref().map(|c| c.storage.clone());
			let flush_actor = match (warm.as_ref(), warm_config.as_ref()) {
				(Some(warm_storage), Some(warm_cfg)) => {
					let actor_ref = FlushActor::spawn(
						&actor_system,
						storage.clone(),
						warm_storage.clone(),
						warm_cfg.flush_interval,
					);
					config.event_bus.register::<MultiCommittedEvent, _>(FlushEventListener::new(
						actor_ref.clone(),
					));
					Some(actor_ref)
				}
				_ => None,
			};
			(warm, flush_actor)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (warm, flush_actor): (Option<WarmStorage>, Option<ActorRef<FlushMessage>>) = {
			let _ = config.warm;
			(None, None)
		};

		Ok(Self(Arc::new(StandardMultiStoreInner {
			hot,
			warm,
			cold,
			drop_actor,
			flush_actor,
			_actor_system: actor_system,
			event_bus: config.event_bus,
		})))
	}

	pub fn hot(&self) -> Option<&HotStorage> {
		self.hot.as_ref()
	}

	pub fn warm(&self) -> Option<&WarmStorage> {
		self.warm.as_ref()
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
			hot: Some(HotConfig {
				storage: HotStorage::memory(),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap()
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_warm_sqlite() -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::testing_memory_with_warm_sqlite_with_eventbus(EventBus::new(&actor_system))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_warm_sqlite_with_eventbus(event_bus: EventBus) -> Self {
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		Self::new(MultiStoreConfig {
			hot: Some(HotConfig {
				storage: HotStorage::memory(),
			}),
			warm: Some(WarmConfig::sqlite_in_memory()),
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			actor_system,
			clock: Clock::Real,
		})
		.unwrap()
	}
}
