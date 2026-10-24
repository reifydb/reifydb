// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::lifecycle::{progress::Progress, task::LifecycleTask};
use reifydb_runtime::{
	actor::{
		context::Context,
		system::{ActorConfig, ActorHandle, ActorSpawner},
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	sync::{mutex::Mutex, waiter::WaiterHandle},
};
use reifydb_value::value::duration::Duration;
use tracing::{debug, instrument};

const CATCHUP_DELAY: Duration = Duration::from_milliseconds_const(5);

#[instrument(name = "lifecycle::actor::tick", level = "debug", skip_all, fields(class = task.name()))]
fn run_slice(task: &mut Box<dyn LifecycleTask>) -> Progress {
	task.run_slice()
}

#[instrument(name = "lifecycle::actor::drain", level = "debug", skip_all, fields(class = task.name()))]
fn drain(task: &mut Box<dyn LifecycleTask>) {
	while task.run_slice() == Progress::Yielded {}
}

#[derive(Clone)]
pub enum LifecycleMessage {
	Tick,
	CatchUp,

	RunToExhaustion {
		waiter: Arc<WaiterHandle>,
	},

	SetInterval {
		interval: Duration,
	},

	Shutdown,
}

pub struct LifecycleActor {
	task: Mutex<Option<Box<dyn LifecycleTask>>>,
	catchup: Duration,
}

pub struct LifecycleActorState {
	task: Box<dyn LifecycleTask>,
	timer: Option<TimerHandle>,
	catching_up: bool,
}

impl LifecycleActor {
	pub fn new(task: Box<dyn LifecycleTask>) -> Self {
		Self {
			task: Mutex::new(Some(task)),
			catchup: CATCHUP_DELAY,
		}
	}

	fn slice(&self, state: &mut LifecycleActorState, ctx: &Context<LifecycleMessage>) {
		if run_slice(&mut state.task) == Progress::Yielded {
			state.catching_up = true;
			ctx.schedule_once(self.catchup, || LifecycleMessage::CatchUp);
		}
	}

	pub fn spawn(
		spawner: &ActorSpawner,
		name: &str,
		task: Box<dyn LifecycleTask>,
	) -> ActorHandle<LifecycleMessage> {
		spawner.spawn_maintenance(name, Self::new(task))
	}
}

impl Actor for LifecycleActor {
	type State = LifecycleActorState;
	type Message = LifecycleMessage;

	fn init(&self, ctx: &Context<LifecycleMessage>) -> LifecycleActorState {
		let task = self.task.lock().take().expect("a lifecycle actor initializes exactly once");
		debug!(task = task.name(), "lifecycle task registered");
		let timer = ctx.schedule_tick(task.interval(), move |_nanos| LifecycleMessage::Tick);
		LifecycleActorState {
			task,
			timer: Some(timer),
			catching_up: false,
		}
	}

	fn handle(
		&self,
		state: &mut LifecycleActorState,
		msg: LifecycleMessage,
		ctx: &Context<LifecycleMessage>,
	) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}
		match msg {
			LifecycleMessage::Tick => {
				if !state.catching_up {
					self.slice(state, ctx);
				}
				return Directive::Yield;
			}
			LifecycleMessage::CatchUp => {
				state.catching_up = false;
				self.slice(state, ctx);
				return Directive::Yield;
			}
			LifecycleMessage::RunToExhaustion {
				waiter,
			} => {
				drain(&mut state.task);
				waiter.notify();
			}
			LifecycleMessage::SetInterval {
				interval,
			} => {
				if let Some(handle) = state.timer.take() {
					handle.cancel();
				}
				state.timer = Some(ctx.schedule_tick(interval, move |_nanos| LifecycleMessage::Tick));
			}
			LifecycleMessage::Shutdown => {
				return Directive::Stop;
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(256)
	}
}
