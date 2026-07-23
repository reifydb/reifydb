// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::lifecycle::{progress::Progress, task::LifecycleTask};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
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
	Tick(usize),

	RunToExhaustion {
		index: usize,
		waiter: Arc<WaiterHandle>,
	},

	SetInterval {
		index: usize,
		interval: Duration,
	},

	Shutdown,
}

pub struct LifecycleActor {
	tasks: Mutex<Option<Vec<Box<dyn LifecycleTask>>>>,
	catchup: Duration,
}

pub struct LifecycleActorState {
	tasks: Vec<Box<dyn LifecycleTask>>,
	timers: Vec<Option<TimerHandle>>,
}

impl LifecycleActor {
	pub fn new(tasks: Vec<Box<dyn LifecycleTask>>) -> Self {
		Self {
			tasks: Mutex::new(Some(tasks)),
			catchup: CATCHUP_DELAY,
		}
	}

	pub fn spawn(spawner: &ActorSpawner, tasks: Vec<Box<dyn LifecycleTask>>) -> ActorRef<LifecycleMessage> {
		let actor = Self::new(tasks);
		spawner.spawn_maintenance("lifecycle", actor).actor_ref().clone()
	}
}

impl Actor for LifecycleActor {
	type State = LifecycleActorState;
	type Message = LifecycleMessage;

	fn init(&self, ctx: &Context<LifecycleMessage>) -> LifecycleActorState {
		let tasks = self.tasks.lock().take().unwrap_or_default();
		let mut timers = Vec::with_capacity(tasks.len());
		for (index, task) in tasks.iter().enumerate() {
			debug!(task = task.name(), "lifecycle task registered");
			let timer = ctx.schedule_tick(task.interval(), move |_nanos| LifecycleMessage::Tick(index));
			timers.push(Some(timer));
		}
		LifecycleActorState {
			tasks,
			timers,
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
			LifecycleMessage::Tick(index) => {
				if let Some(task) = state.tasks.get_mut(index)
					&& run_slice(task) == Progress::Yielded
				{
					ctx.schedule_once(self.catchup, move || LifecycleMessage::Tick(index));
				}
			}
			LifecycleMessage::RunToExhaustion {
				index,
				waiter,
			} => {
				if let Some(task) = state.tasks.get_mut(index) {
					drain(task);
				}
				waiter.notify();
			}
			LifecycleMessage::SetInterval {
				index,
				interval,
			} => {
				if let Some(slot) = state.timers.get_mut(index) {
					if let Some(handle) = slot.take() {
						handle.cancel();
					}
					*slot = Some(ctx
						.schedule_tick(interval, move |_nanos| LifecycleMessage::Tick(index)));
				}
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
