// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::Debug,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

#[cfg(reifydb_target = "dst")]
pub(crate) mod dst;
#[cfg(reifydb_target = "native")]
pub mod scheduler;
#[cfg(reifydb_target = "wasi")]
pub(crate) mod wasi;
#[cfg(reifydb_target = "wasm")]
pub(crate) mod wasm;

#[cfg(reifydb_target = "wasi")]
use wasi::drain_expired_timers as wasi_drain;

use super::mailbox::SendError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repeat {
	Keep,

	Cancel,
}

impl Repeat {
	pub fn after_send<M>(result: Result<(), SendError<M>>) -> Self {
		match result {
			Ok(()) => Self::Keep,
			Err(SendError::Full(_)) => Self::Keep,
			Err(SendError::Closed(_)) => Self::Cancel,
		}
	}

	pub fn is_keep(&self) -> bool {
		matches!(self, Self::Keep)
	}
}

#[derive(Clone)]
pub struct TimerHandle {
	id: u64,
	cancelled: Arc<AtomicBool>,
}

impl TimerHandle {
	pub(crate) fn new(id: u64) -> Self {
		Self {
			id,
			cancelled: Arc::new(AtomicBool::new(false)),
		}
	}

	pub fn cancel(&self) -> bool {
		self.cancelled.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok()
	}

	pub fn is_cancelled(&self) -> bool {
		self.cancelled.load(Ordering::SeqCst)
	}

	pub fn id(&self) -> u64 {
		self.id
	}

	pub(crate) fn cancelled_flag(&self) -> Arc<AtomicBool> {
		self.cancelled.clone()
	}
}

impl Debug for TimerHandle {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("TimerHandle").field("id", &self.id).field("cancelled", &self.is_cancelled()).finish()
	}
}

static TIMER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn next_timer_id() -> u64 {
	TIMER_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(reifydb_target = "wasi")]
pub fn drain_expired_timers() {
	wasi_drain();
}

#[cfg(not(reifydb_target = "wasi"))]
pub fn drain_expired_timers() {}

#[cfg(all(test, reifydb_target = "native"))]
mod tests {
	use super::*;
	use crate::actor::mailbox::create_mailbox;

	#[test]
	fn a_full_mailbox_drops_the_tick_but_keeps_the_timer_armed() {
		// The defect this pins: every repeating timer collapsed send() into `.is_ok()`, so one
		// transient full mailbox retired the timer for the life of the process with no log line.
		// Six of the nine lifecycle maintenance tasks were dead this way, the tombstone reaper
		// among them, which is how operator state came to be 80% unreaped tombstones.
		let (actor, _mailbox) = create_mailbox::<u8>(Some(1));
		assert_eq!(Repeat::after_send(actor.send(1)), Repeat::Keep, "precondition: the first send fits");

		let full = actor.send(2);
		assert!(matches!(full, Err(SendError::Full(_))), "precondition: capacity 1 must reject the second send");
		assert_eq!(Repeat::after_send(full), Repeat::Keep, "backpressure must not retire a repeating timer");
	}

	#[test]
	fn a_closed_mailbox_retires_the_timer() {
		// The one case that must still cancel. Without it a timer fires forever into a dead
		// channel, and nothing else ever removes it from the heap.
		let (actor, mailbox) = create_mailbox::<u8>(Some(1));
		drop(mailbox);

		let closed = actor.send(1);
		assert!(matches!(closed, Err(SendError::Closed(_))), "precondition: a dropped mailbox closes the channel");
		assert_eq!(Repeat::after_send(closed), Repeat::Cancel, "a dead actor must retire its timer");
	}
}
