// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_runtime::actor::system::ActorSpawner;
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::ActorConfig,
	traits::{Actor, Directive},
};
use tracing::debug;

use crate::tier::resident::OperatorResidentState;

#[derive(Clone)]
pub enum EvictMessage {
	Pressure,
	Shutdown,
}

pub struct ResidentEvictActor {
	buffer: OperatorResidentState,
}

impl ResidentEvictActor {
	pub fn new(buffer: OperatorResidentState) -> Self {
		Self {
			buffer,
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn spawn(spawner: &ActorSpawner, buffer: OperatorResidentState) -> ActorRef<EvictMessage> {
		let actor = Self::new(buffer);
		spawner.spawn_coordination("operator-resident-evict", actor).actor_ref().clone()
	}
}

impl Actor for ResidentEvictActor {
	type State = ();
	type Message = EvictMessage;

	fn init(&self, ctx: &Context<EvictMessage>) {
		let _ = ctx;
		debug!("Operator resident evict actor started");
	}

	fn handle(&self, _state: &mut (), msg: EvictMessage, ctx: &Context<EvictMessage>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}
		match msg {
			EvictMessage::Pressure => {
				self.buffer.evict_to_capacity();
			}
			EvictMessage::Shutdown => {
				debug!("Operator resident evict actor shutting down");
				return Directive::Stop;
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("Operator resident evict actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}
