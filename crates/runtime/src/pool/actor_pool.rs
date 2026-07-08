// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::VecDeque,
	mem,
	panic::{AssertUnwindSafe, catch_unwind},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread,
};

use crossbeam_channel::Sender;
use tracing::error;

use crate::{
	pool::task::TaskItem,
	sync::{condvar::Condvar, mutex::Mutex},
};

pub(crate) const COORDINATION_BATCH_SIZE: usize = 64;
pub(crate) const FLOW_BATCH_SIZE: usize = 8;
pub(crate) const EPHEMERAL_BATCH_SIZE: usize = 64;

pub(crate) trait Runnable: Send + Sync + 'static {
	fn run(self: Arc<Self>);
}

pub(crate) struct Worker {
	queue: Mutex<VecDeque<Arc<dyn Runnable>>>,
	condvar: Condvar,
}

impl Worker {
	fn new() -> Self {
		Self {
			queue: Mutex::new(VecDeque::new()),
			condvar: Condvar::new(),
		}
	}

	pub(crate) fn push(&self, item: Arc<dyn Runnable>) {
		self.queue.lock().push_back(item);
		self.condvar.notify_one();
	}

	fn try_steal(&self) -> Option<Arc<dyn Runnable>> {
		self.queue.try_lock().and_then(|mut queue| queue.pop_front())
	}
}

#[derive(Clone)]
pub(crate) enum Schedule {
	Pinned(Arc<Worker>),
	Injector(Sender<TaskItem>),
}

impl Schedule {
	pub(crate) fn enqueue(&self, item: Arc<dyn Runnable>) {
		match self {
			Schedule::Pinned(worker) => worker.push(item),
			Schedule::Injector(tx) => {
				let _ = tx.send(TaskItem::Actor(item));
			}
		}
	}
}

pub(crate) struct WorkerGroup {
	workers: Vec<Arc<Worker>>,
	next: AtomicUsize,
	shutdown: Arc<AtomicBool>,
	joins: Mutex<Vec<thread::JoinHandle<()>>>,
	batch_size: usize,
}

impl WorkerGroup {
	fn new(threads: usize, name_prefix: &'static str, batch_size: usize) -> Self {
		let shutdown = Arc::new(AtomicBool::new(false));
		let workers: Vec<Arc<Worker>> = (0..threads).map(|_| Arc::new(Worker::new())).collect();

		let joins = workers
			.iter()
			.enumerate()
			.map(|(i, worker)| {
				let me = Arc::clone(worker);
				let siblings: Vec<Arc<Worker>> = workers
					.iter()
					.enumerate()
					.filter(|(j, _)| *j != i)
					.map(|(_, sibling)| Arc::clone(sibling))
					.collect();
				let shutdown = Arc::clone(&shutdown);
				thread::Builder::new()
					.name(format!("{name_prefix}-{i}"))
					.spawn(move || worker_loop(me, siblings, shutdown))
					.unwrap_or_else(|_| panic!("failed to spawn {name_prefix} worker thread"))
			})
			.collect();

		Self {
			workers,
			next: AtomicUsize::new(0),
			shutdown,
			joins: Mutex::new(joins),
			batch_size,
		}
	}

	pub(crate) fn assign(&self) -> Arc<Worker> {
		let i = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
		Arc::clone(&self.workers[i])
	}

	pub(crate) fn batch_size(&self) -> usize {
		self.batch_size
	}

	pub(crate) fn thread_count(&self) -> usize {
		self.workers.len()
	}

	fn shutdown_and_join(&self) {
		if self.shutdown.swap(true, Ordering::AcqRel) {
			return;
		}
		for worker in &self.workers {
			let _guard = worker.queue.lock();
			worker.condvar.notify_all();
		}
		let joins = mem::take(&mut *self.joins.lock());
		let current = thread::current().id();
		for handle in joins {
			if handle.thread().id() != current {
				let _ = handle.join();
			}
		}
	}
}

pub(crate) struct ActorPool {
	coordination: WorkerGroup,
	flow: WorkerGroup,
}

impl ActorPool {
	pub(crate) fn new(coordination_threads: usize, flow_threads: usize) -> Self {
		Self {
			coordination: WorkerGroup::new(coordination_threads, "coordination", COORDINATION_BATCH_SIZE),
			flow: WorkerGroup::new(flow_threads, "flow", FLOW_BATCH_SIZE),
		}
	}

	pub(crate) fn coordination(&self) -> &WorkerGroup {
		&self.coordination
	}

	pub(crate) fn flow(&self) -> &WorkerGroup {
		&self.flow
	}

	pub(crate) fn shutdown(&self) {
		self.coordination.shutdown_and_join();
		self.flow.shutdown_and_join();
	}
}

fn worker_loop(me: Arc<Worker>, siblings: Vec<Arc<Worker>>, shutdown: Arc<AtomicBool>) {
	loop {
		match next_item(&me, &siblings, &shutdown) {
			Some(item) => run_guarded(item),
			None => return,
		}
	}
}

fn next_item(me: &Arc<Worker>, siblings: &[Arc<Worker>], shutdown: &Arc<AtomicBool>) -> Option<Arc<dyn Runnable>> {
	loop {
		{
			let mut guard = me.queue.lock();
			if let Some(item) = guard.pop_front() {
				return Some(item);
			}
			if shutdown.load(Ordering::Acquire) {
				return None;
			}
		}

		if let Some(stolen) = steal(siblings) {
			return Some(stolen);
		}

		let mut guard = me.queue.lock();
		if let Some(item) = guard.pop_front() {
			return Some(item);
		}
		if shutdown.load(Ordering::Acquire) {
			return None;
		}
		me.condvar.wait(&mut guard);
	}
}

fn steal(siblings: &[Arc<Worker>]) -> Option<Arc<dyn Runnable>> {
	siblings.iter().find_map(|sibling| sibling.try_steal())
}

fn run_guarded(item: Arc<dyn Runnable>) {
	let result = catch_unwind(AssertUnwindSafe(|| item.run()));
	if result.is_err() {
		error!("actor pool worker caught a panicked actor batch");
	}
}
