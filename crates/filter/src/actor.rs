// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	timers::TimerHandle,
	traits::{Actor, Directive},
};
use reifydb_value::value::duration::Duration;
use tracing::debug;

use crate::{
	adaptive::AdaptiveKeyFilter,
	config::FilterConfig,
	driver::{DriverProgress, RebuildDriver},
	source::KeyFilterSource,
};

const CATCHUP_DELAY: Duration = Duration::from_milliseconds_const(5);

pub enum FilterMessage {
	Register {
		filter: Arc<AdaptiveKeyFilter>,
		source: Box<dyn KeyFilterSource>,
		config: FilterConfig,
	},

	Tick(usize),

	Shutdown,
}

pub struct FilterActor {
	catchup: Duration,
}

pub struct FilterActorState {
	drivers: Vec<RebuildDriver>,
	timers: Vec<Option<TimerHandle>>,
}

impl Default for FilterActor {
	fn default() -> Self {
		Self::new()
	}
}

impl FilterActor {
	pub fn new() -> Self {
		Self {
			catchup: CATCHUP_DELAY,
		}
	}

	pub fn spawn(spawner: &ActorSpawner) -> ActorRef<FilterMessage> {
		spawner.spawn_maintenance("filter", Self::new()).actor_ref().clone()
	}
}

impl Actor for FilterActor {
	type State = FilterActorState;
	type Message = FilterMessage;

	fn init(&self, _ctx: &Context<FilterMessage>) -> FilterActorState {
		FilterActorState {
			drivers: Vec::new(),
			timers: Vec::new(),
		}
	}

	fn handle(&self, state: &mut FilterActorState, msg: FilterMessage, ctx: &Context<FilterMessage>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}
		match msg {
			FilterMessage::Register {
				filter,
				source,
				config,
			} => {
				let index = state.drivers.len();
				let interval = config.interval;
				let driver = RebuildDriver::new(filter, source, config);
				debug!(filter = driver.name(), "filter rebuild driver registered");
				state.drivers.push(driver);
				state.timers.push(Some(
					ctx.schedule_tick(interval, move |_nanos| FilterMessage::Tick(index))
				));
				ctx.schedule_once(self.catchup, move || FilterMessage::Tick(index));
			}
			FilterMessage::Tick(index) => {
				let Some(driver) = state.drivers.get_mut(index) else {
					return Directive::Continue;
				};
				let progress = driver.step();
				match progress {
					DriverProgress::Started | DriverProgress::Scanning => {
						ctx.schedule_once(self.catchup, move || FilterMessage::Tick(index));
					}
					DriverProgress::Committed => {
						debug!(filter = driver.name(), "filter rebuild committed");
					}
					DriverProgress::Idle => {}
				}
			}
			FilterMessage::Shutdown => {
				return Directive::Stop;
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(256)
	}
}
