// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	panic::{AssertUnwindSafe, catch_unwind},
	sync::Arc,
};

use reifydb_runtime::{
	actor::{
		context::Context,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	sync::{mutex::Mutex, waiter::WaiterHandle},
};
use tracing::error;

use super::{
	ADVANCER_CHUNK,
	state::{AdvanceOutcome, WatermarkShared, WatermarkState},
};

pub struct AdvanceKick;

pub struct WatermarkAdvancer {
	state: Arc<Mutex<WatermarkState>>,
	shared: Arc<WatermarkShared>,
}

impl WatermarkAdvancer {
	pub fn new(state: Arc<Mutex<WatermarkState>>, shared: Arc<WatermarkShared>) -> Self {
		Self {
			state,
			shared,
		}
	}
}

pub struct AdvancerScratch {
	to_notify: Vec<Arc<WaiterHandle>>,
}

impl Actor for WatermarkAdvancer {
	type State = AdvancerScratch;
	type Message = AdvanceKick;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		AdvancerScratch {
			to_notify: Vec::new(),
		}
	}

	fn handle(&self, scratch: &mut Self::State, _msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		let outcome = {
			let mut state = self.state.lock();
			state.advance_chunk(&self.shared.done_until, &mut scratch.to_notify, ADVANCER_CHUNK)
		};
		for waiter in scratch.to_notify.drain(..) {
			notify_guarded(&waiter);
		}
		match outcome {
			AdvanceOutcome::MoreWork => {
				let _ = ctx.self_ref().send(AdvanceKick);
				Directive::Yield
			}
			AdvanceOutcome::Complete => Directive::Continue,
		}
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(1)
	}
}

fn notify_guarded(waiter: &Arc<WaiterHandle>) {
	if catch_unwind(AssertUnwindSafe(|| waiter.notify())).is_err() {
		error!("watermark waiter callback panicked; the advancer continues draining");
	}
}
