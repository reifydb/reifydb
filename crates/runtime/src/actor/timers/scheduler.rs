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

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use reifydb_value::reifydb_assertions;

use super::{Repeat, TimerHandle, next_timer_id};
use crate::{actor::context::CancellationToken, sync::mutex::Mutex};

struct TimerEntry {
	id: u64,

	deadline: Instant,

	kind: TimerKind,

	cancelled: Arc<AtomicBool>,

	scope: CancellationToken,
}

enum TimerKind {
	Once {
		callback: Box<dyn FnOnce() + Send>,
	},

	Repeat {
		callback: Arc<dyn Fn() -> Repeat + Send + Sync>,
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
		deadline: Instant,
		callback: Box<dyn FnOnce() + Send>,
		cancelled: Arc<AtomicBool>,
		scope: CancellationToken,
	},

	ScheduleRepeat {
		id: u64,
		deadline: Instant,
		interval: Duration,
		callback: Arc<dyn Fn() -> Repeat + Send + Sync>,
		cancelled: Arc<AtomicBool>,
		scope: CancellationToken,
	},

	Purge,

	Shutdown,
}

pub struct SchedulerHandle {
	command_tx: Sender<SchedulerCommand>,
	join_handle: Mutex<Option<JoinHandle<()>>>,
	scope: CancellationToken,
}

impl SchedulerHandle {
	pub fn new() -> Self {
		let (command_tx, command_rx) = unbounded();

		let join_handle = thread::Builder::new()
			.name("timer-scheduler".to_string())
			.spawn(move || {
				scheduler_loop(command_rx);
			})
			.expect("failed to spawn timer scheduler thread");

		Self {
			command_tx,
			join_handle: Mutex::new(Some(join_handle)),
			scope: CancellationToken::new(),
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
			deadline: Instant::now() + delay,
			callback: Box::new(callback),
			cancelled,
			scope: self.scope.clone(),
		});

		handle
	}

	pub fn schedule_repeat<F>(&self, interval: Duration, callback: F) -> TimerHandle
	where
		F: Fn() -> Repeat + Send + Sync + 'static,
	{
		let id = next_timer_id();
		let handle = TimerHandle::new(id);
		let cancelled = handle.cancelled_flag();

		let _ = self.command_tx.send(SchedulerCommand::ScheduleRepeat {
			id,
			deadline: Instant::now() + interval,
			interval,
			callback: Arc::new(callback),
			cancelled,
			scope: self.scope.clone(),
		});

		handle
	}

	pub fn shared(&self, scope: CancellationToken) -> Self {
		Self {
			command_tx: self.command_tx.clone(),
			join_handle: Mutex::new(None),
			scope,
		}
	}

	pub fn shutdown(&self) {
		let Some(handle) = self.join_handle.lock().take() else {
			let _ = self.command_tx.send(SchedulerCommand::Purge);
			return;
		};

		let _ = self.command_tx.send(SchedulerCommand::Shutdown);

		if handle.thread().id() == thread::current().id() {
			return;
		}

		let _ = handle.join();
	}
}

impl Default for SchedulerHandle {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for SchedulerHandle {
	fn drop(&mut self) {
		self.shutdown();
	}
}

fn scheduler_loop(command_rx: Receiver<SchedulerCommand>) {
	let mut heap: BinaryHeap<TimerEntry> = BinaryHeap::new();

	loop {
		let command = match next_command(&command_rx, &heap) {
			ControlFlow::Break(()) => return,
			ControlFlow::Continue(command) => command,
		};

		if let Some(cmd) = command {
			match apply_command(cmd, &mut heap) {
				ControlFlow::Break(()) => return,
				ControlFlow::Continue(()) => {}
			}
		}

		drain_due_timers(&mut heap);
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
fn apply_command(cmd: SchedulerCommand, heap: &mut BinaryHeap<TimerEntry>) -> ControlFlow<(), ()> {
	match cmd {
		SchedulerCommand::ScheduleOnce {
			id,
			deadline,
			callback,
			cancelled,
			scope,
		} => {
			heap.push(TimerEntry {
				id,
				deadline,
				kind: TimerKind::Once {
					callback,
				},
				cancelled,
				scope,
			});
			ControlFlow::Continue(())
		}
		SchedulerCommand::ScheduleRepeat {
			id,
			deadline,
			interval,
			callback,
			cancelled,
			scope,
		} => {
			heap.push(TimerEntry {
				id,
				deadline,
				kind: TimerKind::Repeat {
					callback,
					interval,
				},
				cancelled,
				scope,
			});
			ControlFlow::Continue(())
		}
		SchedulerCommand::Purge => {
			heap.retain(|entry| !entry.scope.is_cancelled());
			ControlFlow::Continue(())
		}
		SchedulerCommand::Shutdown => ControlFlow::Break(()),
	}
}

#[inline]
fn drain_due_timers(heap: &mut BinaryHeap<TimerEntry>) {
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
				run_once_guarded(callback);
			}
			TimerKind::Repeat {
				callback,
				interval,
			} => {
				if run_repeat_guarded(&callback) == Repeat::Cancel {
					entry.cancelled.store(true, Ordering::SeqCst);
				}

				if !entry.cancelled.load(Ordering::SeqCst) {
					heap.push(TimerEntry {
						id: entry.id,
						deadline: now + interval,
						kind: TimerKind::Repeat {
							callback,
							interval,
						},
						cancelled: entry.cancelled,
						scope: entry.scope,
					});
				}
			}
		}
	}
}

#[inline]
fn run_once_guarded(callback: Box<dyn FnOnce() + Send>) {
	callback()
}

#[inline]
fn run_repeat_guarded(callback: &Arc<dyn Fn() -> Repeat + Send + Sync>) -> Repeat {
	callback()
}

#[cfg(test)]
mod tests {
	use std::sync::{atomic::AtomicUsize, mpsc};

	use super::*;
	use crate::sync::mutex::Mutex;

	#[test]
	fn test_schedule_once() {
		let scheduler = SchedulerHandle::new();

		let (tx, rx) = mpsc::channel();
		scheduler.schedule_once(Duration::from_millis(10), move || {
			tx.send(()).unwrap();
		});

		rx.recv_timeout(Duration::from_secs(1)).unwrap();
		scheduler.shutdown();
	}

	#[test]
	fn test_schedule_once_zero_delay() {
		let scheduler = SchedulerHandle::new();

		let (tx, rx) = mpsc::channel();
		scheduler.schedule_once(Duration::ZERO, move || {
			tx.send(()).unwrap();
		});

		rx.recv_timeout(Duration::from_secs(1)).unwrap();
		scheduler.shutdown();
	}

	#[test]
	fn test_schedule_repeat() {
		let scheduler = SchedulerHandle::new();

		let counter = Arc::new(AtomicUsize::new(0));
		let counter_clone = counter.clone();

		let handle = scheduler.schedule_repeat(Duration::from_millis(10), move || {
			counter_clone.fetch_add(1, Ordering::SeqCst);
			Repeat::Keep
		});

		// Polled to a deadline rather than slept for a fixed window, so a "never repeats"
		// regression fails loud instead of racing the sleep.
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
	fn test_schedule_repeat_stops_on_cancel() {
		let scheduler = SchedulerHandle::new();

		let counter = Arc::new(AtomicUsize::new(0));
		let counter_clone = counter.clone();

		scheduler.schedule_repeat(Duration::from_millis(10), move || {
			let count = counter_clone.fetch_add(1, Ordering::SeqCst);
			// Returning Cancel must stop the rearm; the sleep below is long enough for
			// many more iterations if it does not.
			match count < 3 {
				true => Repeat::Keep,
				false => Repeat::Cancel,
			}
		});

		thread::sleep(Duration::from_millis(100));

		let count = counter.load(Ordering::SeqCst);
		assert!(count <= 4, "Expected at most 4 iterations, got {}", count);

		scheduler.shutdown();
	}

	#[test]
	fn test_cancel_before_fire() {
		let scheduler = SchedulerHandle::new();

		let (tx, rx) = mpsc::channel();
		let handle = scheduler.schedule_once(Duration::from_millis(50), move || {
			tx.send(()).unwrap();
		});

		handle.cancel();

		// The timeout outlasts the 50ms deadline, so a cancel that did not take would deliver.
		assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

		scheduler.shutdown();
	}

	#[test]
	fn test_multiple_timers() {
		let scheduler = SchedulerHandle::new();

		let results = Arc::new(Mutex::new(Vec::new()));

		for i in 0..5 {
			let results_clone = results.clone();
			// Scheduled in reverse deadline order, so firing in schedule order is visible.
			let delay = Duration::from_millis((5 - i) * 10);
			scheduler.schedule_once(delay, move || {
				results_clone.lock().push(i);
			});
		}

		thread::sleep(Duration::from_millis(100));

		let results = results.lock();
		assert_eq!(*results, vec![4, 3, 2, 1, 0]);

		scheduler.shutdown();
	}

	#[test]
	fn test_callback_runs_on_scheduler_thread() {
		let scheduler = SchedulerHandle::new();

		let (tx, rx) = mpsc::channel();
		scheduler.schedule_once(Duration::from_millis(5), move || {
			let name = thread::current().name().map(|n| n.to_string());
			tx.send(name).unwrap();
		});

		let name = rx.recv_timeout(Duration::from_secs(1)).unwrap();
		assert_eq!(name.as_deref(), Some("timer-scheduler"));
		scheduler.shutdown();
	}

	#[test]
	fn a_scope_shutdown_drops_its_timers_at_once_not_at_their_deadline() {
		// A stopped scope's hour-long tick otherwise pins its callback and actor channel for that hour.
		let scheduler = SchedulerHandle::new();
		let scope = CancellationToken::new();
		let scoped = scheduler.shared(scope.clone());

		let stopped = Arc::new(());
		let in_stopped = Arc::clone(&stopped);
		scoped.schedule_repeat(Duration::from_secs(3600), move || {
			let _ = &in_stopped;
			Repeat::Keep
		});
		let live = Arc::new(());
		let in_live = Arc::clone(&live);
		scheduler.schedule_repeat(Duration::from_secs(3600), move || {
			let _ = &in_live;
			Repeat::Keep
		});

		scope.cancel();
		scoped.shutdown();

		let deadline = Instant::now() + Duration::from_secs(1);
		while Arc::strong_count(&stopped) > 1 && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(1));
		}
		assert_eq!(Arc::strong_count(&stopped), 1, "a cancelled scope's timer must leave the heap on shutdown");
		assert_eq!(Arc::strong_count(&live), 2, "a timer outside the cancelled scope must stay armed");
		scheduler.shutdown();
	}

	#[test]
	fn test_reentrant_schedule_from_callback() {
		let scheduler = SchedulerHandle::new();
		let shared = scheduler.shared(CancellationToken::new());

		let (tx, rx) = mpsc::channel();
		scheduler.schedule_once(Duration::from_millis(5), move || {
			shared.schedule_once(Duration::from_millis(5), move || {
				tx.send(()).unwrap();
			});
		});

		rx.recv_timeout(Duration::from_secs(1)).unwrap();
		scheduler.shutdown();
	}

	#[test]
	fn test_drop_from_callback_detaches_instead_of_self_joining() {
		// A callback releasing the last owner must never join the scheduler thread from itself: EDEADLK.
		let owner = Arc::new(Mutex::new(Some(SchedulerHandle::new())));
		let (tx, rx) = mpsc::channel();

		let callback_owner = Arc::clone(&owner);
		owner.lock().as_ref().unwrap().schedule_once(Duration::from_millis(5), move || {
			callback_owner.lock().take();
			tx.send(()).unwrap();
		});

		rx.recv_timeout(Duration::from_secs(1)).unwrap();
		assert!(owner.lock().is_none());
	}
}
