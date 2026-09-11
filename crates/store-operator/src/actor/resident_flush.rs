// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::system::ActorSpawner;
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	sync::waiter::WaiterHandle,
};
use reifydb_value::value::duration::Duration;
use tracing::debug;

use crate::resident::Resident;

const FLUSH_PENDING_TIMEOUT: Duration = Duration::from_seconds_const(5);

#[derive(Clone)]
pub enum FlushMessage {
	Pressure,
	Tick,
	Checkpoint {
		flow: FlowId,
		version: CommitVersion,
	},
	Shutdown,
	FlushPending {
		waiter: Arc<WaiterHandle>,
	},
}

pub struct ResidentFlushActor {
	buffer: Resident,
	interval: Duration,
}

impl ResidentFlushActor {
	pub fn new(buffer: Resident, interval: Duration) -> Self {
		Self {
			buffer,
			interval,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn spawn(spawner: &ActorSpawner, buffer: Resident, interval: Duration) -> ActorRef<FlushMessage> {
		let actor = Self::new(buffer, interval);
		spawner.spawn_coordination("operator-persistent-flush", actor).actor_ref().clone()
	}

	fn drain(&self) {
		flush_now(&self.buffer);
	}

	fn arm(&self, ctx: &Context<FlushMessage>) -> TimerHandle {
		ctx.schedule_once(self.interval, || FlushMessage::Tick)
	}

	fn rearm(&self, state: &mut Option<TimerHandle>, ctx: &Context<FlushMessage>) {
		if let Some(pending) = state.take() {
			pending.cancel();
		}
		*state = Some(self.arm(ctx));
	}
}

pub fn flush_now(buffer: &Resident) {
	buffer.flush_all();
}

pub fn flush_pending(actor_ref: &ActorRef<FlushMessage>) -> bool {
	let waiter = Arc::new(WaiterHandle::new());
	if actor_ref
		.send_blocking(FlushMessage::FlushPending {
			waiter: Arc::clone(&waiter),
		})
		.is_err()
	{
		return false;
	}
	waiter.wait_timeout(FLUSH_PENDING_TIMEOUT)
}

impl Actor for ResidentFlushActor {
	type State = Option<TimerHandle>;
	type Message = FlushMessage;

	fn init(&self, ctx: &Context<FlushMessage>) -> Option<TimerHandle> {
		debug!("Operator persistent flush actor started");
		Some(self.arm(ctx))
	}

	fn handle(&self, state: &mut Option<TimerHandle>, msg: FlushMessage, ctx: &Context<FlushMessage>) -> Directive {
		if ctx.is_cancelled() {
			self.drain();
			if let FlushMessage::FlushPending {
				waiter,
			} = msg
			{
				waiter.notify();
			}
			return Directive::Stop;
		}
		match msg {
			FlushMessage::Pressure => {
				self.drain();
				self.rearm(state, ctx);
			}
			FlushMessage::Tick => {
				self.buffer.note_tick();
				self.drain();
				self.rearm(state, ctx);
			}
			FlushMessage::Checkpoint {
				..
			} => {}
			FlushMessage::Shutdown => {
				debug!("Operator persistent flush actor shutting down");
				self.drain();
				return Directive::Stop;
			}
			FlushMessage::FlushPending {
				waiter,
			} => {
				self.drain();
				waiter.notify();
				self.rearm(state, ctx);
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Operator persistent flush actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}
