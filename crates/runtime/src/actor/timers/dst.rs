// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cell::RefCell,
	cmp::Ordering as CmpOrdering,
	collections::BinaryHeap,
	rc::Rc,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use super::{TimerHandle, next_timer_id};
use crate::{actor::mailbox::ActorRef, context::clock::MockClock};

/// A timer entry in the DST timer heap.
pub(crate) struct DstTimerEntry {
	id: u64,
	pub(crate) deadline_nanos: u64,
	kind: DstTimerKind,
	cancelled: Arc<AtomicBool>,
}

enum DstTimerKind {
	Once {
		callback: Box<dyn FnOnce()>,
	},
	Repeat {
		callback: Rc<dyn Fn() -> bool>,
		interval_nanos: u64,
	},
}

/// Shared timer heap type.
pub(crate) type DstTimerHeap = Rc<RefCell<BinaryHeap<DstTimerEntry>>>;

/// Create a new empty timer heap.
pub(crate) fn new_timer_heap() -> DstTimerHeap {
	Rc::new(RefCell::new(BinaryHeap::new()))
}

// Min-heap ordering: smallest deadline first.
// BinaryHeap is a max-heap, so we reverse the comparison.
impl PartialEq for DstTimerEntry {
	fn eq(&self, other: &Self) -> bool {
		self.deadline_nanos == other.deadline_nanos && self.id == other.id
	}
}

impl Eq for DstTimerEntry {}

impl PartialOrd for DstTimerEntry {
	fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
		Some(self.cmp(other))
	}
}

impl Ord for DstTimerEntry {
	fn cmp(&self, other: &Self) -> CmpOrdering {
		// Reverse for min-heap: smaller deadline = higher priority
		other.deadline_nanos.cmp(&self.deadline_nanos).then_with(|| other.id.cmp(&self.id))
	}
}

/// Schedule a one-shot timer that sends a message after a delay.
pub(crate) fn schedule_once_fn<M: 'static, F: FnOnce() -> M + 'static>(
	heap: &DstTimerHeap,
	clock: &MockClock,
	actor_ref: ActorRef<M>,
	delay: Duration,
	factory: F,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();
	let deadline_nanos = clock.now_nanos() + delay.as_nanos() as u64;

	heap.borrow_mut().push(DstTimerEntry {
		id: handle.id(),
		deadline_nanos,
		kind: DstTimerKind::Once {
			callback: Box::new(move || {
				let _ = actor_ref.send(factory());
			}),
		},
		cancelled,
	});

	handle
}

/// Schedule a repeating timer using a factory function.
pub(crate) fn schedule_repeat_fn<M: 'static, F: Fn() -> M + 'static>(
	heap: &DstTimerHeap,
	clock: &MockClock,
	actor_ref: ActorRef<M>,
	interval: Duration,
	factory: F,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();
	let interval_nanos = interval.as_nanos() as u64;
	let deadline_nanos = clock.now_nanos() + interval_nanos;

	let actor_ref = Rc::new(actor_ref);
	let factory = Rc::new(factory);

	let actor_ref_clone = actor_ref.clone();
	let factory_clone = factory.clone();
	let callback: Rc<dyn Fn() -> bool> = Rc::new(move || actor_ref_clone.send(factory_clone()).is_ok());

	heap.borrow_mut().push(DstTimerEntry {
		id: handle.id(),
		deadline_nanos,
		kind: DstTimerKind::Repeat {
			callback,
			interval_nanos,
		},
		cancelled,
	});

	handle
}

/// Schedule a repeating timer that sends a cloned message.
pub(crate) fn schedule_repeat<M: Clone + 'static>(
	heap: &DstTimerHeap,
	clock: &MockClock,
	actor_ref: ActorRef<M>,
	interval: Duration,
	msg: M,
) -> TimerHandle {
	schedule_repeat_fn(heap, clock, actor_ref, interval, move || msg.clone())
}

/// Fire all timers whose deadline is <= `now_nanos`.
///
/// Timers are fired in deadline order. Repeat timers are re-inserted
/// with their next deadline. Returns the number of timers fired.
pub(crate) fn fire_due_timers(heap: &DstTimerHeap, now_nanos: u64) -> usize {
	let mut fired = 0;

	loop {
		// Peek to check if the next timer is due.
		let should_pop = heap.borrow().peek().map_or(false, |entry| entry.deadline_nanos <= now_nanos);

		if !should_pop {
			break;
		}

		let entry = heap.borrow_mut().pop().unwrap();

		// Skip cancelled timers.
		if entry.cancelled.load(Ordering::SeqCst) {
			continue;
		}

		match entry.kind {
			DstTimerKind::Once {
				callback,
			} => {
				callback();
				fired += 1;
			}
			DstTimerKind::Repeat {
				ref callback,
				interval_nanos,
			} => {
				let continue_repeating = callback();
				fired += 1;

				if continue_repeating && !entry.cancelled.load(Ordering::SeqCst) {
					let next_deadline = entry.deadline_nanos + interval_nanos;
					heap.borrow_mut().push(DstTimerEntry {
						id: next_timer_id(),
						deadline_nanos: next_deadline,
						kind: DstTimerKind::Repeat {
							callback: callback.clone(),
							interval_nanos,
						},
						cancelled: entry.cancelled.clone(),
					});
				}
			}
		}
	}

	fired
}
