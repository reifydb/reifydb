// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod tests;

use std::sync::Arc;

use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		traits::{Actor, Directive},
	},
	sync::waiter::WaiterHandle,
};
use reifydb_value::value::duration::Duration;
use tracing::debug;

use crate::tier::commit::OperatorCommitBuffer;

const FLUSH_PENDING_TIMEOUT: Duration = Duration::from_seconds_const(5);

#[derive(Clone)]
pub enum FlushMessage {
	Pressure,
	Shutdown,
	FlushPending {
		waiter: Arc<WaiterHandle>,
	},
}

pub struct OperatorFlushActor {
	buffer: OperatorCommitBuffer,
}

impl OperatorFlushActor {
	pub fn new(buffer: OperatorCommitBuffer) -> Self {
		Self {
			buffer,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn spawn(spawner: &ActorSpawner, buffer: OperatorCommitBuffer) -> ActorRef<FlushMessage> {
		let actor = Self::new(buffer);
		spawner.spawn_coordination("operator-persistent-flush", actor).actor_ref().clone()
	}

	fn drain(&self) {
		flush_now(&self.buffer);
	}

	fn relieve(&self) {
		self.buffer.evict_under_cap();
	}
}

pub fn flush_now(buffer: &OperatorCommitBuffer) {
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

impl Actor for OperatorFlushActor {
	type State = ();
	type Message = FlushMessage;

	fn init(&self, ctx: &Context<FlushMessage>) {
		let _ = ctx;
		debug!("Operator persistent flush actor started");
	}

	fn handle(&self, _state: &mut (), msg: FlushMessage, ctx: &Context<FlushMessage>) -> Directive {
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
				self.relieve();
			}
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
