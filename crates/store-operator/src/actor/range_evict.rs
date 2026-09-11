// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::mailbox::ActorRef;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::system::ActorSpawner;
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	timers::TimerHandle,
	traits::{Actor, Directive},
};
use reifydb_value::value::duration::Duration;
use tracing::debug;

use crate::range::tiers::RangeTiers;

#[derive(Clone)]
pub enum RangeEvictMessage {
	Tick,
	Shutdown,
}

pub struct RangeEvictActor {
	tiers: RangeTiers,
	interval: Duration,
}

impl RangeEvictActor {
	pub fn new(tiers: RangeTiers, interval: Duration) -> Self {
		Self {
			tiers,
			interval,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn spawn(spawner: &ActorSpawner, tiers: RangeTiers, interval: Duration) -> ActorRef<RangeEvictMessage> {
		let actor = Self::new(tiers, interval);
		spawner.spawn_maintenance("operator-range-evict", actor).actor_ref().clone()
	}

	fn arm(&self, ctx: &Context<RangeEvictMessage>) -> TimerHandle {
		ctx.schedule_once(self.interval, || RangeEvictMessage::Tick)
	}

	fn rearm(&self, state: &mut Option<TimerHandle>, ctx: &Context<RangeEvictMessage>) {
		if let Some(pending) = state.take() {
			pending.cancel();
		}
		*state = Some(self.arm(ctx));
	}
}

impl Actor for RangeEvictActor {
	type State = Option<TimerHandle>;
	type Message = RangeEvictMessage;

	fn init(&self, ctx: &Context<RangeEvictMessage>) -> Option<TimerHandle> {
		debug!("Operator range evict actor started");
		Some(self.arm(ctx))
	}

	fn handle(
		&self,
		state: &mut Option<TimerHandle>,
		msg: RangeEvictMessage,
		ctx: &Context<RangeEvictMessage>,
	) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}
		match msg {
			RangeEvictMessage::Tick => {
				self.tiers.relieve();
				self.rearm(state, ctx);
			}
			RangeEvictMessage::Shutdown => {
				debug!("Operator range evict actor shutting down");
				return Directive::Stop;
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Operator range evict actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}
