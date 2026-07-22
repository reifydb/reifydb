// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::take, sync::Arc};

use reifydb_value::value::duration::Duration;
use tracing::debug;

use crate::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	sync::{mutex::Mutex, waiter::WaiterHandle},
};

const CATCHUP_DELAY: Duration = Duration::from_milliseconds_const(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
	Yielded,
	Exhausted,
}

impl Progress {
	pub fn is_yielded(self) -> bool {
		matches!(self, Progress::Yielded)
	}

	pub fn is_exhausted(self) -> bool {
		matches!(self, Progress::Exhausted)
	}
}

pub trait MaintenanceTask: Send + 'static {
	fn name(&self) -> &'static str;

	fn interval(&self) -> Duration;

	fn run_slice(&mut self) -> Progress;
}

#[derive(Clone)]
pub struct MaintenanceRegistry {
	tasks: Arc<Mutex<Vec<Box<dyn MaintenanceTask>>>>,
}

impl MaintenanceRegistry {
	pub fn new() -> Self {
		Self {
			tasks: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn register(&self, task: Box<dyn MaintenanceTask>) {
		self.tasks.lock().push(task);
	}

	pub fn take(&self) -> Vec<Box<dyn MaintenanceTask>> {
		take(&mut *self.tasks.lock())
	}

	pub fn is_empty(&self) -> bool {
		self.tasks.lock().is_empty()
	}
}

impl Default for MaintenanceRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Clone)]
pub enum MaintenanceMessage {
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

pub struct MaintenanceActor {
	tasks: Mutex<Option<Vec<Box<dyn MaintenanceTask>>>>,
	catchup: Duration,
}

pub struct MaintenanceActorState {
	tasks: Vec<Box<dyn MaintenanceTask>>,
	timers: Vec<Option<TimerHandle>>,
}

impl MaintenanceActor {
	pub fn new(tasks: Vec<Box<dyn MaintenanceTask>>) -> Self {
		Self {
			tasks: Mutex::new(Some(tasks)),
			catchup: CATCHUP_DELAY,
		}
	}

	pub fn spawn(spawner: &ActorSpawner, tasks: Vec<Box<dyn MaintenanceTask>>) -> ActorRef<MaintenanceMessage> {
		let actor = Self::new(tasks);
		spawner.spawn_maintenance("maintenance", actor).actor_ref().clone()
	}
}

impl Actor for MaintenanceActor {
	type State = MaintenanceActorState;
	type Message = MaintenanceMessage;

	fn init(&self, ctx: &Context<MaintenanceMessage>) -> MaintenanceActorState {
		let tasks = self.tasks.lock().take().unwrap_or_default();
		let mut timers = Vec::with_capacity(tasks.len());
		for (index, task) in tasks.iter().enumerate() {
			debug!(task = task.name(), "maintenance task registered");
			let timer = ctx.schedule_tick(task.interval(), move |_nanos| MaintenanceMessage::Tick(index));
			timers.push(Some(timer));
		}
		MaintenanceActorState {
			tasks,
			timers,
		}
	}

	fn handle(
		&self,
		state: &mut MaintenanceActorState,
		msg: MaintenanceMessage,
		ctx: &Context<MaintenanceMessage>,
	) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}
		match msg {
			MaintenanceMessage::Tick(index) => {
				if let Some(task) = state.tasks.get_mut(index)
					&& task.run_slice() == Progress::Yielded
				{
					ctx.schedule_once(self.catchup, move || MaintenanceMessage::Tick(index));
				}
			}
			MaintenanceMessage::RunToExhaustion {
				index,
				waiter,
			} => {
				if let Some(task) = state.tasks.get_mut(index) {
					while task.run_slice() == Progress::Yielded {}
				}
				waiter.notify();
			}
			MaintenanceMessage::SetInterval {
				index,
				interval,
			} => {
				if let Some(slot) = state.timers.get_mut(index) {
					if let Some(handle) = slot.take() {
						handle.cancel();
					}
					*slot = Some(ctx.schedule_tick(interval, move |_nanos| {
						MaintenanceMessage::Tick(index)
					}));
				}
			}
			MaintenanceMessage::Shutdown => {
				return Directive::Stop;
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(256)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	struct CountingTask {
		remaining: usize,
		slices: Arc<Mutex<usize>>,
	}

	impl MaintenanceTask for CountingTask {
		fn name(&self) -> &'static str {
			"counting"
		}

		fn interval(&self) -> Duration {
			Duration::from_seconds(1).unwrap()
		}

		fn run_slice(&mut self) -> Progress {
			*self.slices.lock() += 1;
			if self.remaining > 0 {
				self.remaining -= 1;
			}
			if self.remaining == 0 {
				Progress::Exhausted
			} else {
				Progress::Yielded
			}
		}
	}

	#[test]
	fn registry_collects_and_drains_tasks() {
		let registry = MaintenanceRegistry::new();
		assert!(registry.is_empty(), "a fresh registry holds no tasks");
		registry.register(Box::new(CountingTask {
			remaining: 1,
			slices: Arc::new(Mutex::new(0)),
		}));
		assert!(!registry.is_empty(), "a registered task must be visible before the actor drains it");
		let taken = registry.take();
		assert_eq!(taken.len(), 1, "take must hand every registered task to the actor exactly once");
		assert!(registry.is_empty(), "take must empty the registry so the actor is the sole owner");
	}

	#[test]
	fn run_slice_reports_yielded_until_work_is_exhausted() {
		// The maintenance actor reschedules a catch-up only while a task reports Yielded; a task that
		// under-reports (returns Exhausted with work left) would silently strand its backlog until the
		// next full interval, so the budget/yield contract is the load-bearing invariant here.
		let slices = Arc::new(Mutex::new(0usize));
		let mut task = CountingTask {
			remaining: 3,
			slices: slices.clone(),
		};
		assert_eq!(task.run_slice(), Progress::Yielded, "first of three slices still has work left");
		assert_eq!(task.run_slice(), Progress::Yielded, "second slice still has work left");
		assert_eq!(task.run_slice(), Progress::Exhausted, "the final slice must report Exhausted");
		assert_eq!(*slices.lock(), 3, "each slice must run exactly once");
	}
}
