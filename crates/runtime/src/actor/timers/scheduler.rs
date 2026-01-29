// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	cmp::Ordering as CmpOrdering,
	collections::BinaryHeap,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use rayon::ThreadPool;

use super::{TimerHandle, next_timer_id};

struct TimerEntry {
	/// Unique timer ID.
	id: u64,
	/// When the timer should fire.
	deadline: Instant,
	/// The kind of timer
	kind: TimerKind,
	/// Shared flag to check if cancelled.
	cancelled: Arc<AtomicBool>,
}

enum TimerKind {
	/// Fire once and remove.
	Once {
		callback: Box<dyn FnOnce() + Send>,
	},
	/// Fire repeatedly until cancelled or callback returns false.
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
	// BinaryHeap is a max-heap, so we reverse the ordering to get a min-heap by deadline.
	fn cmp(&self, other: &Self) -> CmpOrdering {
		// Reverse ordering for min-heap behavior
		other.deadline.cmp(&self.deadline).then_with(|| other.id.cmp(&self.id))
	}
}

impl PartialOrd for TimerEntry {
	fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
		Some(self.cmp(other))
	}
}

/// Commands sent to the scheduler coordinator thread.
enum SchedulerCommand {
	/// Schedule a one-shot timer.
	ScheduleOnce {
		id: u64,
		delay: Duration,
		callback: Box<dyn FnOnce() + Send>,
		cancelled: Arc<AtomicBool>,
	},
	/// Schedule a repeating timer.
	ScheduleRepeat {
		id: u64,
		interval: Duration,
		callback: Arc<dyn Fn() -> bool + Send + Sync>,
		cancelled: Arc<AtomicBool>,
	},
	/// Shutdown the scheduler.
	Shutdown,
}

/// Handle to the timer scheduler.
///
/// Used to schedule timers and shutdown the scheduler.
/// Cloning the handle creates another reference to the same scheduler.
pub struct SchedulerHandle {
	command_tx: Sender<SchedulerCommand>,
	join_handle: Option<JoinHandle<()>>,
}

impl SchedulerHandle {
	/// Create and start a new scheduler.
	///
	/// The scheduler runs on a dedicated coordinator thread and dispatches
	/// timer callbacks to the provided rayon thread pool.
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

	/// Schedule a callback to fire once after a delay.
	///
	/// Returns a handle that can be used to cancel the timer.
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

	/// Schedule a callback to fire repeatedly at an interval.
	///
	/// The callback returns `true` to continue or `false` to stop.
	/// Returns a handle that can be used to cancel the timer.
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

	/// Shutdown the scheduler and wait for it to complete.
	pub fn shutdown(&mut self) {
		let _ = self.command_tx.send(SchedulerCommand::Shutdown);

		if let Some(handle) = self.join_handle.take() {
			let _ = handle.join();
		}
	}
}

impl Drop for SchedulerHandle {
	fn drop(&mut self) {
		// Signal shutdown if not already done
		let _ = self.command_tx.send(SchedulerCommand::Shutdown);
		// Note: We don't join here to avoid blocking in drop
	}
}

/// The main scheduler loop running on the coordinator thread.
fn scheduler_loop(command_rx: Receiver<SchedulerCommand>, pool: Arc<ThreadPool>) {
	let mut heap: BinaryHeap<TimerEntry> = BinaryHeap::new();

	loop {
		// Calculate timeout until next timer
		let timeout = heap.peek().map(|entry| {
			let now = Instant::now();
			if entry.deadline <= now {
				Duration::ZERO
			} else {
				entry.deadline.duration_since(now)
			}
		});

		// Wait for command or timeout
		let command = match timeout {
			Some(Duration::ZERO) => {
				// Timer(s) ready to fire - check for commands without blocking
				command_rx.try_recv().ok()
			}
			Some(dur) => {
				// Wait until next timer or command
				match command_rx.recv_timeout(dur) {
					Ok(cmd) => Some(cmd),
					Err(RecvTimeoutError::Timeout) => None,
					Err(RecvTimeoutError::Disconnected) => {
						// Channel closed, exit
						return;
					}
				}
			}
			None => {
				// No timers - block until command
				match command_rx.recv() {
					Ok(cmd) => Some(cmd),
					Err(_) => return, // Channel closed
				}
			}
		};

		// Process command if received
		if let Some(cmd) = command {
			match cmd {
				SchedulerCommand::ScheduleOnce {
					id,
					delay,
					callback,
					cancelled,
				} => {
					let deadline = if delay.is_zero() {
						// Zero delay - fire immediately
						if !cancelled.load(Ordering::SeqCst) {
							pool.spawn(callback);
						}
						continue;
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
				}
				SchedulerCommand::Shutdown => {
					return;
				}
			}
		}

		// Fire all due timers
		let now = Instant::now();
		while let Some(entry) = heap.peek() {
			if entry.deadline > now {
				break;
			}

			let entry = heap.pop().unwrap();

			// Check if cancelled
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

					// Dispatch callback to pool
					pool.spawn(move || {
						if !cancelled.load(Ordering::SeqCst) {
							let continue_timer = callback_clone();
							if !continue_timer {
								cancelled.store(true, Ordering::SeqCst);
							}
						}
					});

					// Re-schedule if not cancelled
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
}

#[cfg(test)]
mod tests {
	use std::sync::{atomic::AtomicUsize, mpsc};

	use super::*;

	fn test_pool() -> Arc<ThreadPool> {
		Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap())
	}

	#[test]
	fn test_schedule_once() {
		let pool = test_pool();
		let mut scheduler = SchedulerHandle::new(pool);

		let (tx, rx) = mpsc::channel();
		scheduler.schedule_once(Duration::from_millis(10), move || {
			tx.send(()).unwrap();
		});

		rx.recv_timeout(Duration::from_secs(1)).unwrap();
		scheduler.shutdown();
	}

	#[test]
	fn test_schedule_once_zero_delay() {
		let pool = test_pool();
		let mut scheduler = SchedulerHandle::new(pool);

		let (tx, rx) = mpsc::channel();
		scheduler.schedule_once(Duration::ZERO, move || {
			tx.send(()).unwrap();
		});

		rx.recv_timeout(Duration::from_secs(1)).unwrap();
		scheduler.shutdown();
	}

	#[test]
	fn test_schedule_repeat() {
		let pool = test_pool();
		let mut scheduler = SchedulerHandle::new(pool);

		let counter = Arc::new(AtomicUsize::new(0));
		let counter_clone = counter.clone();

		let handle = scheduler.schedule_repeat(Duration::from_millis(10), move || {
			counter_clone.fetch_add(1, Ordering::SeqCst);
			true // Continue
		});

		// Wait for several iterations
		thread::sleep(Duration::from_millis(50));
		handle.cancel();

		let count = counter.load(Ordering::SeqCst);
		assert!(count >= 3, "Expected at least 3 iterations, got {}", count);

		scheduler.shutdown();
	}

	#[test]
	fn test_schedule_repeat_stops_on_false() {
		let pool = test_pool();
		let mut scheduler = SchedulerHandle::new(pool);

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
		let pool = test_pool();
		let mut scheduler = SchedulerHandle::new(pool);

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
		let pool = test_pool();
		let mut scheduler = SchedulerHandle::new(pool);

		let results = Arc::new(std::sync::Mutex::new(Vec::new()));

		for i in 0..5 {
			let results_clone = results.clone();
			let delay = Duration::from_millis((5 - i) * 10); // Reverse order
			scheduler.schedule_once(delay, move || {
				results_clone.lock().unwrap().push(i);
			});
		}

		thread::sleep(Duration::from_millis(100));

		let results = results.lock().unwrap();
		// Timers should fire in deadline order (4, 3, 2, 1, 0)
		assert_eq!(*results, vec![4, 3, 2, 1, 0]);

		scheduler.shutdown();
	}
}
