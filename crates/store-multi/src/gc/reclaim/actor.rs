// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::multi_reclaim::MultiReclaimMessage as Message,
	interface::catalog::config::{ConfigKey, GetConfig},
};
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	timers::TimerHandle,
	traits::{Actor as ActorTrait, Directive},
};
use reifydb_value::value::datetime::DateTime;
use tracing::{debug, warn};

use crate::store::StandardMultiStore;

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
}

pub struct Actor {
	store: StandardMultiStore,
	config: Arc<dyn GetConfig>,
}

impl Actor {
	pub fn new(store: StandardMultiStore, config: Arc<dyn GetConfig>) -> Self {
		Self {
			store,
			config,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		store: StandardMultiStore,
		config: Arc<dyn GetConfig>,
	) -> ActorRef<Message> {
		let actor = Self::new(store, config);
		spawner.spawn_background("persistent-reclaim", actor).actor_ref().clone()
	}

	fn reclaim(&self) {
		let Some(persistent) = self.store.persistent() else {
			return;
		};
		if let Err(e) = persistent.reclaim() {
			warn!(error = %e, "persistent free-page reclaim failed");
		}
	}
}

impl ActorTrait for Actor {
	type State = ActorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> ActorState {
		debug!("Persistent reclaim actor started");
		let interval = self.config.get_config_duration(ConfigKey::MultiReclaimInterval);
		let timer_handle = ctx.schedule_tick(interval, |nanos| Message::Tick(DateTime::from_nanos(nanos)));
		ActorState {
			_timer_handle: Some(timer_handle),
		}
	}

	fn handle(&self, _state: &mut ActorState, msg: Message, ctx: &Context<Message>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}

		match msg {
			Message::Tick(_) => self.reclaim(),
			Message::Shutdown => {
				debug!("Persistent reclaim actor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Yield
	}

	fn post_stop(&self) {
		debug!("Persistent reclaim actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

pub fn spawn_persistent_reclaim_actor(
	store: StandardMultiStore,
	spawner: ActorSpawner,
	config: Arc<dyn GetConfig>,
) -> ActorRef<Message> {
	Actor::spawn(&spawner, store, config)
}
