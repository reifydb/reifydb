// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	thread,
	time::Duration as StdDuration,
};

use reifydb_core::lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::Pools,
	sync::{mutex::Mutex, waiter::WaiterHandle},
};
use reifydb_sub_lifecycle::actor::{LifecycleActor, LifecycleMessage};
use reifydb_value::value::duration::Duration;

struct Fast {
	slices: Arc<AtomicUsize>,
	work: Arc<AtomicUsize>,
}

impl LifecycleTask for Fast {
	fn name(&self) -> &'static str {
		"fast"
	}

	fn interval(&self) -> Duration {
		Duration::from_milliseconds(1).unwrap()
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[]
	}

	fn run_slice(&mut self) -> Progress {
		self.slices.fetch_add(1, Ordering::SeqCst);
		let left = self.work.load(Ordering::SeqCst);
		if left == 0 {
			return Progress::Exhausted;
		}
		self.work.store(left - 1, Ordering::SeqCst);
		if left - 1 > 0 {
			Progress::Yielded
		} else {
			Progress::Exhausted
		}
	}
}

struct Stall {
	sleeps: Arc<Mutex<Vec<u64>>>,
}

impl LifecycleTask for Stall {
	fn name(&self) -> &'static str {
		"stall"
	}

	fn interval(&self) -> Duration {
		Duration::from_seconds(600).unwrap()
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[]
	}

	fn run_slice(&mut self) -> Progress {
		let next = {
			let mut sleeps = self.sleeps.lock();
			if sleeps.is_empty() {
				0
			} else {
				sleeps.remove(0)
			}
		};
		thread::sleep(StdDuration::from_millis(next));
		Progress::Exhausted
	}
}

fn attempt() -> (usize, usize, usize) {
	let system = ActorSystem::new(Pools::default(), Clock::Real);
	let spawner = system.spawner();

	let slices = Arc::new(AtomicUsize::new(0));
	let work = Arc::new(AtomicUsize::new(0));
	let sleeps = Arc::new(Mutex::new(vec![400, 0, 400]));

	let stall = LifecycleActor::spawn(
		&spawner,
		"stall",
		Box::new(Stall {
			sleeps: sleeps.clone(),
		}),
	);
	let fast = LifecycleActor::spawn(
		&spawner,
		"fast",
		Box::new(Fast {
			slices: slices.clone(),
			work: work.clone(),
		}),
	);
	thread::sleep(StdDuration::from_millis(50));

	let w1 = Arc::new(WaiterHandle::new());
	let w2 = Arc::new(WaiterHandle::new());
	stall.actor_ref()
		.send(LifecycleMessage::RunToExhaustion {
			waiter: w1,
		})
		.ok()
		.unwrap();
	stall.actor_ref().send(LifecycleMessage::Tick).ok().unwrap();
	stall.actor_ref()
		.send(LifecycleMessage::RunToExhaustion {
			waiter: w2,
		})
		.ok()
		.unwrap();
	thread::sleep(StdDuration::from_millis(100));
	work.store(3, Ordering::SeqCst);

	thread::sleep(StdDuration::from_millis(1000));
	let left = work.load(Ordering::SeqCst);
	let before = slices.load(Ordering::SeqCst);
	thread::sleep(StdDuration::from_millis(300));
	let after = slices.load(Ordering::SeqCst);
	let _ = fast.actor_ref().send(LifecycleMessage::Shutdown);
	let _ = stall.actor_ref().send(LifecycleMessage::Shutdown);
	system.shutdown();
	(left, before, after)
}

#[test]
fn a_task_keeps_slicing_after_a_neighbour_outlasts_its_mailbox() {
	// a catch-up dropped on a full mailbox must not stop a yielding task for good once the shared thread frees up.
	let outcomes: Vec<(usize, usize, usize)> = (0..6).map(|_| attempt()).collect();

	let stuck = outcomes.iter().filter(|(left, _, _)| *left != 0).count();
	assert_eq!(
		stuck, 0,
		"{stuck}/6 attempts stopped with work left (work left, slices, slices 300ms later): {outcomes:?}"
	);
}
