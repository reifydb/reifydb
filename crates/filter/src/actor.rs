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

	fn arm_interval(state: &mut FilterActorState, index: usize, ctx: &Context<FilterMessage>) {
		let Some(driver) = state.drivers.get(index) else {
			return;
		};
		let interval = driver.interval();
		if let Some(slot) = state.timers.get_mut(index) {
			if let Some(handle) = slot.take() {
				handle.cancel();
			}
			*slot = Some(ctx.schedule_once(interval, move || FilterMessage::Tick(index)));
		}
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
				let driver = RebuildDriver::new(filter, source, config);
				debug!(filter = driver.name(), "filter rebuild driver registered");
				state.drivers.push(driver);
				state.timers.push(None);
				Self::arm_interval(state, index, ctx);
				ctx.schedule_once(self.catchup, move || FilterMessage::Tick(index));
			}
			FilterMessage::Tick(index) => {
				let Some(driver) = state.drivers.get_mut(index) else {
					return Directive::Continue;
				};
				let progress = driver.step();
				let name = driver.name();
				match progress {
					DriverProgress::Started | DriverProgress::Scanning => {
						ctx.schedule_once(self.catchup, move || FilterMessage::Tick(index));
					}
					DriverProgress::Committed => {
						debug!(filter = name, "filter rebuild committed");
						Self::arm_interval(state, index, ctx);
					}
					DriverProgress::Idle => {
						Self::arm_interval(state, index, ctx);
					}
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
