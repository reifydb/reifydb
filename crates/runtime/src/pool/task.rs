// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::disallowed_types)]

use std::{
	mem,
	panic::{AssertUnwindSafe, catch_unwind},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	time::Duration,
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError, unbounded};
use tracing::error;

use crate::{pool::actor_pool::Runnable, sync::mutex::Mutex};

pub(crate) enum TaskItem {
	Job(Box<dyn FnOnce() + Send + 'static>),
	Actor(Arc<dyn Runnable>),
}

pub(crate) struct TaskPool {
	tx: Sender<TaskItem>,
	shutdown: Arc<AtomicBool>,
	joins: Mutex<Vec<thread::JoinHandle<()>>>,
	threads: usize,
}

impl TaskPool {
	pub(crate) fn new(threads: usize, name_prefix: &'static str) -> Self {
		let (tx, rx) = unbounded::<TaskItem>();
		let shutdown = Arc::new(AtomicBool::new(false));

		let joins = (0..threads)
			.map(|i| {
				let rx = rx.clone();
				let shutdown = Arc::clone(&shutdown);
				thread::Builder::new()
					.name(format!("{name_prefix}-{i}"))
					.spawn(move || task_loop(rx, shutdown))
					.unwrap_or_else(|_| panic!("failed to spawn {name_prefix} worker thread"))
			})
			.collect();

		Self {
			tx,
			shutdown,
			joins: Mutex::new(joins),
			threads,
		}
	}

	pub(crate) fn spawn(&self, job: impl FnOnce() + Send + 'static) {
		let _ = self.tx.send(TaskItem::Job(Box::new(job)));
	}

	pub(crate) fn injector(&self) -> Sender<TaskItem> {
		self.tx.clone()
	}

	pub(crate) fn thread_count(&self) -> usize {
		self.threads
	}

	pub(crate) fn shutdown(&self) {
		if self.shutdown.swap(true, Ordering::AcqRel) {
			return;
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

fn task_loop(rx: Receiver<TaskItem>, shutdown: Arc<AtomicBool>) {
	loop {
		match rx.try_recv() {
			Ok(item) => run_guarded(item),
			Err(TryRecvError::Empty) => {
				if shutdown.load(Ordering::Acquire) {
					return;
				}
				match rx.recv_timeout(Duration::from_millis(100)) {
					Ok(item) => run_guarded(item),
					Err(RecvTimeoutError::Timeout) => {}
					Err(RecvTimeoutError::Disconnected) => return,
				}
			}
			Err(TryRecvError::Disconnected) => return,
		}
	}
}

fn run_guarded(item: TaskItem) {
	let result = catch_unwind(AssertUnwindSafe(|| match item {
		TaskItem::Job(job) => job(),
		TaskItem::Actor(actor) => actor.run(),
	}));
	if result.is_err() {
		error!("task pool worker caught a panicked job");
	}
}
