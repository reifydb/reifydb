// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(not(reifydb_single_threaded), not(reifydb_dst)))]
#![allow(clippy::disallowed_methods)]
#![allow(clippy::disallowed_types)]

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
		mpsc,
	},
	thread,
	time::{Duration, Instant},
};

use reifydb_runtime::{
	Runtime, RuntimeConfig,
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	pool::PoolConfig,
};

fn runtime(coordination: usize, flow: usize, task: usize) -> Runtime {
	Runtime::from_config(
		RuntimeConfig::default(),
		PoolConfig {
			coordination_threads: coordination,
			flow_threads: flow,
			maintenance_threads: 1,
			task_threads: task,
			compute_threads: 1,
			async_threads: 1,
		},
	)
}

fn wait_until(deadline: Duration, mut check: impl FnMut() -> bool) -> bool {
	let end = Instant::now() + deadline;
	while Instant::now() < end {
		if check() {
			return true;
		}
		thread::sleep(Duration::from_millis(1));
	}
	check()
}

struct CountingActor {
	processed: Arc<AtomicUsize>,
}

impl Actor for CountingActor {
	type State = ();
	type Message = u64;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, _msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		self.processed.fetch_add(1, Ordering::SeqCst);
		Directive::Continue
	}
}

struct SleepActor {
	millis: u64,
	done: Arc<AtomicUsize>,
}

impl Actor for SleepActor {
	type State = ();
	type Message = ();

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, _msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		thread::sleep(Duration::from_millis(self.millis));
		self.done.fetch_add(1, Ordering::SeqCst);
		Directive::Continue
	}
}

struct EchoActor;

impl Actor for EchoActor {
	type State = ();
	type Message = mpsc::Sender<()>;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		let _ = msg.send(());
		Directive::Continue
	}
}

#[test]
fn lost_wake_stress() {
	// A message sent exactly as the actor transitions SCHEDULED -> IDLE must still be
	// processed; a lost wake shows up as processed < sent.
	let rt = runtime(1, 1, 1);
	let processed = Arc::new(AtomicUsize::new(0));
	let handle = rt.spawner().spawn_coordination(
		"lost-wake",
		CountingActor {
			processed: processed.clone(),
		},
	);

	const PRODUCERS: usize = 8;
	const PER_PRODUCER: usize = 25_000;

	let mut joins = Vec::new();
	for p in 0..PRODUCERS {
		let actor_ref = handle.actor_ref().clone();
		joins.push(thread::spawn(move || {
			for i in 0..PER_PRODUCER {
				actor_ref.send(i as u64).unwrap();
				if (i + p) % 97 == 0 {
					thread::yield_now();
				}
			}
		}));
	}
	for j in joins {
		j.join().unwrap();
	}

	let total = PRODUCERS * PER_PRODUCER;
	assert!(
		wait_until(Duration::from_secs(30), || processed.load(Ordering::SeqCst) == total),
		"lost wake: processed {} of {} messages",
		processed.load(Ordering::SeqCst),
		total
	);
}

#[test]
fn group_isolation() {
	// A saturated flow group must never delay a coordination actor; the groups own disjoint
	// worker threads.
	let rt = runtime(2, 2, 1);
	let done = Arc::new(AtomicUsize::new(0));

	let mut flow_handles = Vec::new();
	for i in 0..2 {
		flow_handles.push(rt.spawner().spawn_flow(
			&format!("busy-flow-{i}"),
			SleepActor {
				millis: 300,
				done: done.clone(),
			},
		));
	}
	for handle in &flow_handles {
		handle.actor_ref().send(()).unwrap();
		handle.actor_ref().send(()).unwrap();
	}

	let echo = rt.spawner().spawn_coordination("echo", EchoActor);
	thread::sleep(Duration::from_millis(50));

	let (tx, rx) = mpsc::channel();
	let start = Instant::now();
	echo.actor_ref().send(tx).unwrap();
	rx.recv_timeout(Duration::from_secs(1)).expect("coordination actor starved by flow group");
	let rtt = start.elapsed();

	assert!(rtt < Duration::from_millis(150), "coordination RTT {rtt:?} while flow group saturated");
	assert!(wait_until(Duration::from_secs(5), || done.load(Ordering::SeqCst) == 4));
}

struct SequenceActor {
	last: Arc<Vec<AtomicUsize>>,
	in_handler: Arc<AtomicBool>,
	violations: Arc<AtomicUsize>,
	processed: Arc<AtomicUsize>,
}

impl Actor for SequenceActor {
	type State = ();
	type Message = (usize, usize);

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		if self.in_handler.swap(true, Ordering::SeqCst) {
			self.violations.fetch_add(1, Ordering::SeqCst);
		}

		let (producer, seq) = msg;
		let prev = self.last[producer].swap(seq, Ordering::SeqCst);
		if seq != prev + 1 {
			self.violations.fetch_add(1, Ordering::SeqCst);
		}

		self.in_handler.store(false, Ordering::SeqCst);
		self.processed.fetch_add(1, Ordering::SeqCst);
		Directive::Continue
	}
}

#[test]
fn ordering_and_mutual_exclusion_under_steal() {
	// Per-actor ordering and mutual exclusion must survive an idle worker stealing the actor
	// between batches.
	let rt = runtime(4, 1, 1);

	const PRODUCERS: usize = 4;
	const PER_PRODUCER: usize = 10_000;

	let last: Arc<Vec<AtomicUsize>> = Arc::new((0..PRODUCERS).map(|_| AtomicUsize::new(0)).collect());
	let violations = Arc::new(AtomicUsize::new(0));
	let processed = Arc::new(AtomicUsize::new(0));

	let handle = rt.spawner().spawn_coordination(
		"sequence",
		SequenceActor {
			last: last.clone(),
			in_handler: Arc::new(AtomicBool::new(false)),
			violations: violations.clone(),
			processed: processed.clone(),
		},
	);

	let noise_processed = Arc::new(AtomicUsize::new(0));
	let mut noise = Vec::new();
	for i in 0..8 {
		noise.push(rt.spawner().spawn_coordination(
			&format!("noise-{i}"),
			CountingActor {
				processed: noise_processed.clone(),
			},
		));
	}

	let mut joins = Vec::new();
	for p in 0..PRODUCERS {
		let actor_ref = handle.actor_ref().clone();
		joins.push(thread::spawn(move || {
			for seq in 1..=PER_PRODUCER {
				actor_ref.send((p, seq)).unwrap();
			}
		}));
	}
	for (i, n) in noise.iter().enumerate() {
		for _ in 0..100 {
			n.actor_ref().send(i as u64).unwrap();
		}
	}
	for j in joins {
		j.join().unwrap();
	}

	let total = PRODUCERS * PER_PRODUCER;
	assert!(wait_until(Duration::from_secs(30), || processed.load(Ordering::SeqCst) == total));
	assert_eq!(violations.load(Ordering::SeqCst), 0, "ordering or mutual-exclusion violation detected");
}

struct YieldingActor {
	processed: Arc<AtomicUsize>,
}

impl Actor for YieldingActor {
	type State = ();
	type Message = ();

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, _msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		thread::sleep(Duration::from_millis(1));
		self.processed.fetch_add(1, Ordering::SeqCst);
		Directive::Yield
	}
}

#[test]
fn yield_fairness() {
	// Yield must re-enqueue at the worker's tail, or a backlogged yielding actor monopolizes it.
	let rt = runtime(1, 1, 1);

	let a_processed = Arc::new(AtomicUsize::new(0));
	let a = rt.spawner().spawn_coordination(
		"yielder",
		YieldingActor {
			processed: a_processed.clone(),
		},
	);

	const BACKLOG: usize = 200;
	for _ in 0..BACKLOG {
		a.actor_ref().send(()).unwrap();
	}

	let b = rt.spawner().spawn_coordination("probe", EchoActor);
	let (tx, rx) = mpsc::channel();
	b.actor_ref().send(tx).unwrap();
	rx.recv_timeout(Duration::from_secs(5)).expect("probe starved behind a yielding actor");

	let drained = a_processed.load(Ordering::SeqCst);
	assert!(drained < BACKLOG, "probe only ran after the yielding actor drained its whole backlog ({drained})");
}

#[test]
fn steal_liveness() {
	// An idle sibling worker must steal queued actors from a busy worker in the same group.
	let rt = runtime(1, 2, 1);
	let done = Arc::new(AtomicUsize::new(0));

	// Round-robin pinning puts a and c on worker 0 and b on worker 1.
	let a = rt.spawner().spawn_flow(
		"flow-a",
		SleepActor {
			millis: 300,
			done: done.clone(),
		},
	);
	let b = rt.spawner().spawn_flow(
		"flow-b",
		SleepActor {
			millis: 300,
			done: done.clone(),
		},
	);
	let c = rt.spawner().spawn_flow(
		"flow-c",
		SleepActor {
			millis: 300,
			done: done.clone(),
		},
	);
	thread::sleep(Duration::from_millis(50));

	let start = Instant::now();
	a.actor_ref().send(()).unwrap();
	a.actor_ref().send(()).unwrap();
	b.actor_ref().send(()).unwrap();
	c.actor_ref().send(()).unwrap();

	assert!(wait_until(Duration::from_secs(5), || done.load(Ordering::SeqCst) == 4));
	let elapsed = start.elapsed();

	// Without stealing worker 0 serializes a then c for ~900ms; the 800ms bound sits between
	// that and the ~600ms a successful steal produces.
	assert!(elapsed < Duration::from_millis(800), "steal did not happen: {elapsed:?}");
}

#[test]
fn shutdown_drains_and_joins() {
	// Shutdown must drain queues, stop every actor and join every worker without hanging.
	let rt = runtime(2, 2, 2);
	let processed = Arc::new(AtomicUsize::new(0));

	let mut handles = Vec::new();
	for i in 0..6 {
		let handle = rt.spawner().spawn_coordination(
			&format!("shutdown-{i}"),
			CountingActor {
				processed: processed.clone(),
			},
		);
		for m in 0..1_000 {
			handle.actor_ref().send(m).unwrap();
		}
		handles.push(handle);
	}

	let start = Instant::now();
	drop(rt);
	assert!(start.elapsed() < Duration::from_secs(10), "shutdown hung");
}

#[derive(Debug)]
enum EphemeralMessage {
	Echo(mpsc::Sender<u64>, u64),
	Stop,
}

struct EphemeralActor;

impl Actor for EphemeralActor {
	type State = ();
	type Message = EphemeralMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			EphemeralMessage::Echo(tx, value) => {
				let _ = tx.send(value + 1);
				Directive::Continue
			}
			EphemeralMessage::Stop => Directive::Stop,
		}
	}
}

#[test]
fn ephemeral_lifecycle_and_spawn_task() {
	// Ephemeral actors share the task pool with one-shot spawn_task closures, so neither may
	// starve the other.
	let rt = runtime(1, 1, 2);
	let pools = rt.handle().pools();

	let jobs_done = Arc::new(AtomicUsize::new(0));
	for _ in 0..100 {
		let jobs_done = jobs_done.clone();
		pools.spawn_task(move || {
			jobs_done.fetch_add(1, Ordering::SeqCst);
		});
	}

	let handle = rt.spawner().spawn_ephemeral("request", EphemeralActor);
	let (tx, rx) = mpsc::channel();
	handle.actor_ref().send(EphemeralMessage::Echo(tx, 41)).unwrap();
	assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 42);

	handle.actor_ref().send(EphemeralMessage::Stop).unwrap();
	handle.join().unwrap();

	assert!(wait_until(Duration::from_secs(5), || jobs_done.load(Ordering::SeqCst) == 100));
}
