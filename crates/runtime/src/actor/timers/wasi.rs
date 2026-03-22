// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WASI timer implementation using a thread-local timer queue.
//!
//! Since WASI is single-threaded with no event loop, timers are stored in a
//! queue and drained at strategic points (after each message processing cycle
//! and before each bridge command). Expired timers fire their callbacks
//! synchronously.

use std::{
	cell::{Cell, RefCell},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::{Duration, Instant},
};

use super::{TimerHandle, next_timer_id};
use crate::actor::mailbox::ActorRef;

struct TimerEntry {
	fire_at: Instant,
	callback: Box<dyn FnOnce()>,
	cancelled: Arc<AtomicBool>,
}

thread_local! {
	static TIMER_QUEUE: RefCell<Vec<TimerEntry>> = RefCell::new(Vec::new());
	static DRAINING: Cell<bool> = const { Cell::new(false) };
}

/// Schedule a message to be sent after a delay.
///
/// The message is enqueued and will fire when `drain_expired_timers` runs
/// after the delay has elapsed.
pub fn schedule_once_fn<M: Send + 'static, F: FnOnce() -> M + Send + 'static>(
	actor_ref: ActorRef<M>,
	delay: Duration,
	factory: F,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();
	let fire_at = Instant::now() + delay;

	TIMER_QUEUE.with(|q| {
		q.borrow_mut().push(TimerEntry {
			fire_at,
			callback: Box::new(move || {
				let _ = actor_ref.send(factory());
			}),
			cancelled,
		});
	});

	handle
}

/// Schedule a message to be sent repeatedly at an interval.
///
/// Each firing re-enqueues itself for the next interval.
pub fn schedule_repeat<M: Send + Clone + 'static>(actor_ref: ActorRef<M>, interval: Duration, msg: M) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();

	enqueue_repeat(actor_ref, interval, msg, cancelled);

	handle
}

fn enqueue_repeat<M: Send + Clone + 'static>(
	actor_ref: ActorRef<M>,
	interval: Duration,
	msg: M,
	cancelled: Arc<AtomicBool>,
) {
	let fire_at = Instant::now() + interval;
	let cancelled_for_reschedule = cancelled.clone();

	let callback: Box<dyn FnOnce()> = Box::new({
		let actor_ref_clone = actor_ref.clone();
		let msg_clone = msg.clone();
		move || {
			if actor_ref.send(msg).is_ok() {
				enqueue_repeat(actor_ref_clone, interval, msg_clone, cancelled_for_reschedule);
			}
		}
	});

	TIMER_QUEUE.with(|q| {
		q.borrow_mut().push(TimerEntry {
			fire_at,
			callback,
			cancelled,
		});
	});
}

/// Drain all expired timers, firing their callbacks synchronously.
///
/// Uses a reentrancy guard so that timer callbacks (which may send actor
/// messages, triggering further drains) do not recurse.
pub fn drain_expired_timers() {
	if DRAINING.with(|d| d.get()) {
		return;
	}
	DRAINING.with(|d| d.set(true));

	loop {
		let now = Instant::now();
		let entry = TIMER_QUEUE.with(|q| {
			let mut queue = q.borrow_mut();
			// Remove cancelled entries while scanning
			queue.retain(|e| !e.cancelled.load(Ordering::SeqCst));
			// Find an expired entry
			if let Some(idx) = queue.iter().position(|e| e.fire_at <= now) {
				Some(queue.swap_remove(idx))
			} else {
				None
			}
		});

		match entry {
			Some(entry) => (entry.callback)(),
			None => break,
		}
	}

	DRAINING.with(|d| d.set(false));
}
