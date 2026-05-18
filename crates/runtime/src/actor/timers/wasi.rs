// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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

pub fn schedule_repeat_fn<M: Send + 'static, F: Fn() -> M + Send + 'static>(
	actor_ref: ActorRef<M>,
	interval: Duration,
	factory: F,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();

	enqueue_repeat_fn(actor_ref, interval, factory, cancelled);

	handle
}

fn enqueue_repeat_fn<M: Send + 'static, F: Fn() -> M + Send + 'static>(
	actor_ref: ActorRef<M>,
	interval: Duration,
	factory: F,
	cancelled: Arc<AtomicBool>,
) {
	let fire_at = Instant::now() + interval;
	let cancelled_for_reschedule = cancelled.clone();

	let callback: Box<dyn FnOnce()> = Box::new({
		let actor_ref_clone = actor_ref.clone();
		move || {
			if actor_ref.send(factory()).is_ok() {
				enqueue_repeat_fn(actor_ref_clone, interval, factory, cancelled_for_reschedule);
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

pub fn drain_expired_timers() {
	if DRAINING.with(|d| d.get()) {
		return;
	}
	DRAINING.with(|d| d.set(true));

	loop {
		let now = Instant::now();
		let entry = TIMER_QUEUE.with(|q| {
			let mut queue = q.borrow_mut();

			queue.retain(|e| !e.cancelled.load(Ordering::SeqCst));

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
