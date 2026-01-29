// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc, time::Duration};

use reifydb_core::event::EventBus;
use reifydb_runtime::{
	actor::mailbox::ActorRef,
	actor::system::{ActorSystem, ActorSystemConfig},
	clock::Clock,
};
use tracing::instrument;

use crate::{HotConfig, cold::ColdStorage, config::MultiStoreConfig, hot::storage::HotStorage, warm::WarmStorage};

pub mod drop;
pub mod multi;
pub mod router;
pub mod version;
pub mod worker;

use worker::{DropActor, DropMessage, DropWorkerConfig};

#[derive(Clone)]
pub struct StandardMultiStore(Arc<StandardMultiStoreInner>);

pub struct StandardMultiStoreInner {
	pub(crate) hot: Option<HotStorage>,
	pub(crate) warm: Option<WarmStorage>,
	pub(crate) cold: Option<ColdStorage>,
	/// Reference to the drop actor for sending drop requests.
	pub(crate) drop_actor: ActorRef<DropMessage>,
	/// Actor system that owns the drop actor.
	_actor_system: ActorSystem,
	/// Event bus for emitting storage statistics events.
	pub(crate) event_bus: EventBus,
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::new", level = "debug", skip(config), fields(
		has_hot = config.hot.is_some(),
		has_warm = config.warm.is_some(),
		has_cold = config.cold.is_some(),
	))]
	pub fn new(config: MultiStoreConfig) -> crate::Result<Self> {
		let hot = config.hot.map(|c| c.storage);
		// TODO: warm and cold are placeholders for now
		let warm = None;
		let cold = None;
		let _ = config.warm;
		let _ = config.cold;

		// Create actor system for the drop actor
		let actor_system = ActorSystem::new(ActorSystemConfig::default());

		// Spawn drop actor
		let storage = hot.as_ref().expect("hot tier is required");
		let drop_config = DropWorkerConfig::default();
		let drop_actor = DropActor::spawn(&actor_system, drop_config, storage.clone(), config.event_bus.clone(), Clock::default());

		Ok(Self(Arc::new(StandardMultiStoreInner {
			hot,
			warm,
			cold,
			drop_actor,
			_actor_system: actor_system,
			event_bus: config.event_bus,
		})))
	}

	/// Get access to the hot storage tier.
	///
	/// Returns `None` if the hot tier is not configured.
	pub fn hot(&self) -> Option<&HotStorage> {
		self.hot.as_ref()
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
		Self::testing_memory_with_eventbus(EventBus::new(&ActorSystem::new(ActorSystemConfig::default())))
	}

	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		Self::new(MultiStoreConfig {
			hot: Some(HotConfig {
				storage: HotStorage::memory(),
				retention_period: Duration::from_millis(100),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
		})
		.unwrap()
	}
}
