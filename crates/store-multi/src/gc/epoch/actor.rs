// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::version_epoch::VersionEpochMessage as Message,
	interface::catalog::config::{ConfigKey, GetConfig},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		timers::TimerHandle,
		traits::{Actor as ActorTrait, Directive},
	},
	version_epoch::VersionEpoch,
};
use reifydb_value::value::datetime::DateTime;
use tracing::{debug, trace};

use super::EpochSource;

pub struct ActorState {
	_timer_handle: Option<TimerHandle>,
}

pub struct Actor<S: EpochSource> {
	epoch: VersionEpoch,
	source: S,
	config: Arc<dyn GetConfig>,
}

impl<S: EpochSource> Actor<S> {
	pub fn new(epoch: VersionEpoch, source: S, config: Arc<dyn GetConfig>) -> Self {
		Self {
			epoch,
			source,
			config,
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		epoch: VersionEpoch,
		source: S,
		config: Arc<dyn GetConfig>,
	) -> ActorRef<Message> {
		let actor = Self::new(epoch, source, config);
		spawner.spawn_background("version-epoch-sampler", actor).actor_ref().clone()
	}

	fn sample(&self) {
		let now_nanos = self.source.now_nanos();
		let Some(version) = self.source.current_version() else {
			trace!("Version epoch sampler: no committed version yet, skipping sample");
			return;
		};
		self.epoch.record(now_nanos, version.0);
	}
}

impl<S: EpochSource> ActorTrait for Actor<S> {
	type State = ActorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> ActorState {
		debug!("Version epoch sampler started");
		let interval = self.config.get_config_duration(ConfigKey::VersionEpochSampleInterval);
		let timer_handle = ctx.schedule_tick(interval, |nanos| Message::Tick(DateTime::from_nanos(nanos)));
		self.sample();
		ActorState {
			_timer_handle: Some(timer_handle),
		}
	}

	fn handle(&self, _state: &mut ActorState, msg: Message, ctx: &Context<Message>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}

		match msg {
			Message::Tick(_) => self.sample(),
			Message::Shutdown => {
				debug!("Version epoch sampler shutting down");
				return Directive::Stop;
			}
		}

		Directive::Yield
	}

	fn post_stop(&self) {
		debug!("Version epoch sampler stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

pub fn spawn_version_epoch_sampler<S: EpochSource>(
	epoch: VersionEpoch,
	spawner: ActorSpawner,
	source: S,
	config: Arc<dyn GetConfig>,
) -> ActorRef<Message> {
	Actor::spawn(&spawner, epoch, source, config)
}
