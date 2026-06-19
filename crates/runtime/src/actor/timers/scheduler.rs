// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::disallowed_methods)]
#![allow(clippy::disallowed_types)]

use std::{
	cmp::Ordering as CmpOrdering,
	collections::BinaryHeap,
	ops::ControlFlow,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use rayon::ThreadPool;
use reifydb_value::reifydb_assertions;

use super::{TimerHandle, next_timer_id};

struct TimerEntry {
	id: u64,

	deadline: Instant,

	kind: TimerKind,

	cancelled: Arc<AtomicBool>,
}

enum TimerKind {
	Once {
		callback: Box<dyn FnOnce() + Send>,
	},

	Repeat {
		callback: Arc<dyn Fn() -> bool + Send + Sync>,
		interval: Duration,
	},
}

impl Eq for TimerEntry {}

impl PartialEq for TimerEntry {
	fn eq(&self, other: &Self) -> bool {
		self.deadline == other.deadline && self.id == other.id
	}
}

impl Ord for TimerEntry {
	fn cmp(&self, other: &Self) -> CmpOrdering {
		other.deadline.cmp(&self.deadline).then_with(|| other.id.cmp(&self.id))
	}
}

impl PartialOrd for TimerEntry {
	fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
		Some(self.cmp(other))
	}
}

enum SchedulerCommand {
	ScheduleOnce {
		id: u64,
		delay: Duration,
		callback: Box<dyn FnOnce() + Send>,
		cancelled: Arc<AtomicBool>,
	},

	ScheduleRepeat {
		id: u64,
		interval: Duration,
		callback: Arc<dyn Fn() -> bool + Send + Sync>,
		cancelled: Arc<AtomicBool>,
	},

	Shutdown,
}

pub struct SchedulerHandle {
	command_tx: Sender<SchedulerCommand>,
	join_handle: Option<JoinHandle<()>>,
}

impl SchedulerHandle {
	pub fn new(pool: Arc<ThreadPool>) -> Self {
		let (command_tx, command_rx) = bounded(256);

		let join_handle = thread::Builder::new()
			.name("timer-scheduler".to_string())
			.spawn(move || {
				scheduler_loop(command_rx, pool);
			})
			.expect("failed to spawn timer scheduler thread");

		Self {
			command_tx,
			join_handle: Some(join_handle),
		}
	}

	pub fn schedule_once<F>(&self, delay: Duration, callback: F) -> TimerHandle
	where
		F: FnOnce() + Send + 'static,
	{
		let id = next_timer_id();
		let handle = TimerHandle::new(id);
		let cancelled = handle.cancelled_flag();

		let _ = self.command_tx.send(SchedulerCommand::ScheduleOnce {
			id,
			delay,
			callback: Box::new(callback),
			cancelled,
		});

		handle
	}

	pub fn schedule_repeat<F>(&self, interval: Duration, callback: F) -> TimerHandle
	where
		F: Fn() -> bool + Send + Sync + 'static,
	{
		let id = next_timer_id();
		let handle = TimerHandle::new(id);
		let cancelled = handle.cancelled_flag();

		let _ = self.command_tx.send(SchedulerCommand::ScheduleRepeat {
			id,
			interval,
			callback: Arc::new(callback),
			cancelled,
		});

		handle
	}

	pub fn shared(&self) -> Self {
		Self {
			command_tx: self.command_tx.clone(),
			join_handle: None,
		}
	}

	pub fn shutdown(&mut self) {
		if let Some(handle) = self.join_handle.take() {
			let _ = self.command_tx.send(SchedulerCommand::Shutdown);
			let _ = handle.join();
		}
	}
}

impl Drop for SchedulerHandle {
	fn drop(&mut self) {
		if let Some(handle) = self.join_handle.take() {
			let _ = self.command_tx.send(SchedulerCommand::Shutdown);
			let _ = handle.join();
		}
	}
}

fn scheduler_loop(command_rx: Receiver<SchedulerCommand>, pool: Arc<ThreadPool>) {
	let mut heap: BinaryHeap<TimerEntry> = BinaryHeap::new();

	loop {
		let command = match next_command(&command_rx, &heap) {
			ControlFlow::Break(()) => return,
			ControlFlow::Continue(command) => command,
		};

		if let Some(cmd) = command {
			match apply_command(cmd, &mut heap, &pool) {
				ControlFlow::Break(()) => return,
				ControlFlow::Continue(true) => continue,
				ControlFlow::Continue(false) => {}
			}
		}

		drain_due_timers(&mut heap, &pool);
	}
}

#[inline]
fn next_command(
	command_rx: &Receiver<SchedulerCommand>,
	heap: &BinaryHeap<TimerEntry>,
) -> ControlFlow<(), Option<SchedulerCommand>> {
	let timeout = heap.peek().map(|entry| {
		let now = Instant::now();
		if entry.deadline <= now {
			Duration::ZERO
		} else {
			entry.deadline.duration_since(now)
		}
	});

	match timeout {
		Some(Duration::ZERO) => ControlFlow::Continue(command_rx.try_recv().ok()),
		Some(dur) => match command_rx.recv_timeout(dur) {
			Ok(cmd) => ControlFlow::Continue(Some(cmd)),
			Err(RecvTimeoutError::Timeout) => ControlFlow::Continue(None),
			Err(RecvTimeoutError::Disconnected) => ControlFlow::Break(()),
		},
		None => match command_rx.recv() {
			Ok(cmd) => ControlFlow::Continue(Some(cmd)),
			Err(_) => ControlFlow::Break(()),
		},
	}
}

#[inline]
fn apply_command(
	cmd: SchedulerCommand,
	heap: &mut BinaryHeap<TimerEntry>,
	pool: &Arc<ThreadPool>,
) -> ControlFlow<(), bool> {
	match cmd {
		SchedulerCommand::ScheduleOnce {
			id,
			delay,
			callback,
			cancelled,
		} => {
			let deadline = if delay.is_zero() {
				if !cancelled.load(Ordering::SeqCst) {
					pool.spawn(callback);
				}
				return ControlFlow::Continue(true);
			} else {
				Instant::now() + delay
			};

			heap.push(TimerEntry {
				id,
				deadline,
				kind: TimerKind::Once {
					callback,
				},
				cancelled,
			});
			ControlFlow::Continue(false)
		}
		SchedulerCommand::ScheduleRepeat {
			id,
			interval,
			callback,
			cancelled,
		} => {
			let deadline = Instant::now() + interval;

			heap.push(TimerEntry {
				id,
				deadline,
				kind: TimerKind::Repeat {
					callback,
					interval,
				},
				cancelled,
			});
			ControlFlow::Continue(false)
		}
		SchedulerCommand::Shutdown => ControlFlow::Break(()),
	}
}

#[inline]
fn drain_due_timers(heap: &mut BinaryHeap<TimerEntry>, pool: &Arc<ThreadPool>) {
	let now = Instant::now();
	while let Some(entry) = heap.peek() {
		if entry.deadline > now {
			break;
		}

		reifydb_assertions! {
			let peeked = heap.peek().is_some();
			assert!(
				peeked,
				"timer heap.pop() relies on the immediately-preceding peek seeing a due entry; \
				 an empty heap here would unwrap None and panic the scheduler thread, killing every timer"
			);
		}
		let entry = heap.pop().unwrap();

		if entry.cancelled.load(Ordering::SeqCst) {
			continue;
		}

		match entry.kind {
			TimerKind::Once {
				callback,
			} => {
				pool.spawn(callback);
			}
			TimerKind::Repeat {
				callback,
				interval,
			} => {
				let cancelled = entry.cancelled.clone();
				let callback_clone = callback.clone();

				pool.spawn(move || {
					if !cancelled.load(Ordering::SeqCst) {
						let continue_timer = callback_clone();
						if !continue_timer {
							cancelled.store(true, Ordering::SeqCst);
						}
					}
				});

				if !entry.cancelled.load(Ordering::SeqCst) {
					heap.push(TimerEntry {
						id: entry.id,
						deadline: now + interval,
						kind: TimerKind::Repeat {
							callback,
							interval,
						},
						cancelled: entry.cancelled,
					});
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{atomic::AtomicUsize, mpsc};

	use rayon::ThreadPoolBuilder;

	use crate::sync::mutex::Mutex;

	fn test_pool() -> Arc<ThreadPool> {
		Arc::new(ThreadPoolBuilder::new().num_threads(1).build().unwrap())
	}

	use super::*;

	#[test]
	fn test_schedule_once() {
		let mut scheduler = SchedulerHandle::new(test_pool());

		let (tx, rx) = mpsc::channel();
		scheduler.schedule_once(Duration::from_millis(10), move || {
			tx.send(()).unwrap();
		});

		rx.recv_timeout(Duration::from_secs(1)).unwrap();
		scheduler.shutdown();
	}

	#[test]
	fn test_schedule_once_zero_delay() {
		let mut scheduler = SchedulerHandle::new(test_pool());

		let (tx, rx) = mpsc::channel();
		scheduler.schedule_once(Duration::ZERO, move || {
			tx.send(()).unwrap();
		});

		rx.recv_timeout(Duration::from_secs(1)).unwrap();
		scheduler.shutdown();
	}

	#[test]
	fn test_schedule_repeat() {
		let mut scheduler = SchedulerHandle::new(test_pool());

		let counter = Arc::new(AtomicUsize::new(0));
		let counter_clone = counter.clone();

		let handle = scheduler.schedule_repeat(Duration::from_millis(10), move || {
			counter_clone.fetch_add(1, Ordering::SeqCst);
			true // Continue
		});

		// Wait until the repeating timer has fired several times, with a timeout
		// so a "never repeats" regression still fails loud rather than racing a
		// fixed sleep window.
		let deadline = Instant::now() + Duration::from_secs(5);
		while counter.load(Ordering::SeqCst) < 3 && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(10));
		}
		handle.cancel();

		let count = counter.load(Ordering::SeqCst);
		assert!(count >= 3, "Expected at least 3 iterations, got {}", count);

		scheduler.shutdown();
	}

	#[test]
	fn test_schedule_repeat_stops_on_false() {
		let mut scheduler = SchedulerHandle::new(test_pool());

		let counter = Arc::new(AtomicUsize::new(0));
		let counter_clone = counter.clone();

		scheduler.schedule_repeat(Duration::from_millis(10), move || {
			let count = counter_clone.fetch_add(1, Ordering::SeqCst);
			count < 3 // Stop after 3 iterations
		});

		// Wait enough time for many iterations
		thread::sleep(Duration::from_millis(100));

		// Should have stopped at 3
		let count = counter.load(Ordering::SeqCst);
		assert!(count <= 4, "Expected at most 4 iterations, got {}", count);

		scheduler.shutdown();
	}

	#[test]
	fn test_cancel_before_fire() {
		let mut scheduler = SchedulerHandle::new(test_pool());

		let (tx, rx) = mpsc::channel();
		let handle = scheduler.schedule_once(Duration::from_millis(50), move || {
			tx.send(()).unwrap();
		});

		// Cancel immediately
		handle.cancel();

		// Should not receive anything
		assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

		scheduler.shutdown();
	}

	#[test]
	fn test_multiple_timers() {
		let mut scheduler = SchedulerHandle::new(test_pool());

		let results = Arc::new(Mutex::new(Vec::new()));

		for i in 0..5 {
			let results_clone = results.clone();
			let delay = Duration::from_millis((5 - i) * 10); // Reverse order
			scheduler.schedule_once(delay, move || {
				results_clone.lock().push(i);
			});
		}

		thread::sleep(Duration::from_millis(100));

		let results = results.lock();
		// Timers should fire in deadline order (4, 3, 2, 1, 0)
		assert_eq!(*results, vec![4, 3, 2, 1, 0]);

		scheduler.shutdown();
	}
}
