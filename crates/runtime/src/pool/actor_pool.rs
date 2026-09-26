// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::VecDeque,
	mem,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread,
};

use crossbeam_channel::Sender;

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
	sleeping: AtomicBool,
	poked: AtomicBool,
}

impl Worker {
	fn new() -> Self {
		Self {
			queue: Mutex::new(VecDeque::new()),
			condvar: Condvar::new(),
			sleeping: AtomicBool::new(false),
			poked: AtomicBool::new(false),
		}
	}

	pub(crate) fn push(&self, item: Arc<dyn Runnable>) {
		self.queue.lock().push_back(item);
		self.condvar.notify_one();
	}

	fn try_steal(&self) -> Option<Arc<dyn Runnable>> {
		self.queue.try_lock().and_then(|mut queue| queue.pop_front())
	}

	fn steal(&self) -> Option<Arc<dyn Runnable>> {
		self.queue.lock().pop_front()
	}

	fn is_sleeping(&self) -> bool {
		self.sleeping.load(Ordering::SeqCst)
	}

	fn wake(&self) {
		let _guard = self.queue.lock();
		self.poked.store(true, Ordering::SeqCst);
		self.condvar.notify_one();
	}
}

#[derive(Clone)]
pub(crate) struct Pin {
	worker: Arc<Worker>,
	group: Arc<[Arc<Worker>]>,
}

impl Pin {
	fn push(&self, item: Arc<dyn Runnable>) {
		self.worker.push(item);
		if self.worker.is_sleeping() {
			return;
		}
		wake_one_sleeping(self.group.iter().filter(|worker| !Arc::ptr_eq(worker, &self.worker)));
	}
}

#[derive(Clone)]
pub(crate) enum Schedule {
	Pinned(Pin),
	Injector(Sender<TaskItem>),
}

impl Schedule {
	pub(crate) fn enqueue(&self, item: Arc<dyn Runnable>) {
		match self {
			Schedule::Pinned(pin) => pin.push(item),
			Schedule::Injector(tx) => {
				let _ = tx.send(TaskItem::Actor(item));
			}
		}
	}
}

pub(crate) struct WorkerGroup {
	workers: Arc<[Arc<Worker>]>,
	next: AtomicUsize,
	shutdown: Arc<AtomicBool>,
	joins: Mutex<Vec<thread::JoinHandle<()>>>,
	batch_size: usize,
}

impl WorkerGroup {
	fn new(threads: usize, name_prefix: &'static str, batch_size: usize) -> Self {
		let shutdown = Arc::new(AtomicBool::new(false));
		let workers: Arc<[Arc<Worker>]> = (0..threads).map(|_| Arc::new(Worker::new())).collect();

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

	pub(crate) fn assign(&self) -> Pin {
		let i = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
		self.pin(i)
	}

	fn pin(&self, i: usize) -> Pin {
		Pin {
			worker: Arc::clone(&self.workers[i]),
			group: Arc::clone(&self.workers),
		}
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
		for worker in self.workers.iter() {
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
	maintenance: WorkerGroup,
}

impl ActorPool {
	pub(crate) fn new(coordination_threads: usize, flow_threads: usize, maintenance_threads: usize) -> Self {
		Self {
			coordination: WorkerGroup::new(coordination_threads, "coordination", COORDINATION_BATCH_SIZE),
			flow: WorkerGroup::new(flow_threads, "flow", FLOW_BATCH_SIZE),
			maintenance: WorkerGroup::new(
				maintenance_threads.max(1),
				"maintenance",
				COORDINATION_BATCH_SIZE,
			),
		}
	}

	pub(crate) fn coordination(&self) -> &WorkerGroup {
		&self.coordination
	}

	pub(crate) fn flow(&self) -> &WorkerGroup {
		&self.flow
	}

	pub(crate) fn maintenance(&self) -> &WorkerGroup {
		&self.maintenance
	}

	pub(crate) fn shutdown(&self) {
		self.coordination.shutdown_and_join();
		self.flow.shutdown_and_join();
		self.maintenance.shutdown_and_join();
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

		me.sleeping.store(true, Ordering::SeqCst);
		if let Some(stolen) = siblings.iter().find_map(|sibling| sibling.steal()) {
			return Some(wake_up(me, siblings, stolen));
		}

		let mut guard = me.queue.lock();
		if let Some(item) = guard.pop_front() {
			drop(guard);
			return Some(wake_up(me, siblings, item));
		}
		if shutdown.load(Ordering::Acquire) {
			me.sleeping.store(false, Ordering::SeqCst);
			return None;
		}
		if !me.poked.swap(false, Ordering::SeqCst) {
			me.condvar.wait(&mut guard);
			me.poked.store(false, Ordering::SeqCst);
		}
		me.sleeping.store(false, Ordering::SeqCst);
	}
}

fn wake_up(me: &Worker, siblings: &[Arc<Worker>], item: Arc<dyn Runnable>) -> Arc<dyn Runnable> {
	me.sleeping.store(false, Ordering::SeqCst);
	if !me.queue.lock().is_empty() {
		wake_one_sleeping(siblings.iter());
	}
	item
}

fn wake_one_sleeping<'a>(mut workers: impl Iterator<Item = &'a Arc<Worker>>) {
	if let Some(sleeper) = workers.find(|worker| worker.is_sleeping()) {
		sleeper.wake();
	}
}

fn steal(siblings: &[Arc<Worker>]) -> Option<Arc<dyn Runnable>> {
	siblings.iter().find_map(|sibling| sibling.try_steal())
}

fn run_guarded(item: Arc<dyn Runnable>) {
	item.run()
}

#[cfg(test)]
mod tests {
	use std::{
		sync::mpsc::{Sender as StdSender, channel},
		thread,
	};

	use reifydb_value::value::duration::Duration;

	use super::*;

	struct Job(Mutex<Option<Box<dyn FnOnce() + Send>>>);

	impl Runnable for Job {
		fn run(self: Arc<Self>) {
			if let Some(job) = self.0.lock().take() {
				job();
			}
		}
	}

	fn job(f: impl FnOnce() + Send + 'static) -> Arc<dyn Runnable> {
		Arc::new(Job(Mutex::new(Some(Box::new(f)))))
	}

	fn report_thread(tx: StdSender<String>) -> impl FnOnce() + Send + 'static {
		move || {
			let _ = tx.send(thread::current().name().unwrap_or_default().to_string());
		}
	}

	#[test]
	fn work_queued_behind_a_blocked_thread_runs_on_a_sleeping_sibling() {
		let group = WorkerGroup::new(2, "starve", COORDINATION_BATCH_SIZE);
		let (started_tx, started_rx) = channel();
		let (release_tx, release_rx) = channel::<()>();
		group.assign().push(job(move || {
			report_thread(started_tx)();
			let _ = release_rx.recv();
		}));
		let blocked_name = started_rx.recv().unwrap();
		let blocked: usize = blocked_name.strip_prefix("starve-").unwrap().parse().unwrap();
		let sibling = 1 - blocked;
		while !group.workers[sibling].is_sleeping() {
			thread::yield_now();
		}

		let (ran_tx, ran_rx) = channel();
		group.pin(blocked).push(job(report_thread(ran_tx)));
		let ran_on = ran_rx.recv_timeout(Duration::from_seconds_const(10).to_std());

		let _ = release_tx.send(());
		group.shutdown_and_join();
		assert_eq!(
			ran_on.as_deref(),
			Ok(format!("starve-{sibling}").as_str()),
			"a blocked thread never drains its queue, so its sleeping sibling must be woken to steal the item"
		);
	}

	#[test]
	fn work_queued_on_a_sleeping_thread_runs() {
		let group = WorkerGroup::new(2, "own", COORDINATION_BATCH_SIZE);
		for worker in group.workers.iter() {
			while !worker.is_sleeping() {
				thread::yield_now();
			}
		}

		let (ran_tx, ran_rx) = channel();
		group.pin(0).push(job(report_thread(ran_tx)));
		let ran_on = ran_rx.recv_timeout(Duration::from_seconds_const(10).to_std());

		group.shutdown_and_join();
		assert!(
			ran_on.is_ok(),
			"a push to a sleeping owner skips the sibling wake, so the owner itself must run it: {ran_on:?}"
		);
	}
}
