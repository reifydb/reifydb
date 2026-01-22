// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native timer implementation using std::thread.

use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::actor::mailbox::ActorRef;
use super::{next_timer_id, TimerHandle};

/// Schedule a message to be sent after a delay.
///
/// Returns a handle that can be used to cancel the timer.
pub fn schedule_once<M: Send + Clone + 'static>(
	actor_ref: ActorRef<M>,
	delay: Duration,
	msg: M,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();

	std::thread::spawn(move || {
		std::thread::sleep(delay);

		if !cancelled.load(Ordering::SeqCst) {
			let _ = actor_ref.send(msg);
		}
	});

	handle
}

/// Schedule a message to be sent repeatedly at an interval.
///
/// Returns a handle that can be used to cancel the timer.
pub fn schedule_repeat<M: Send + Clone + 'static>(
	actor_ref: ActorRef<M>,
	interval: Duration,
	msg: M,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();

	std::thread::spawn(move || {
		loop {
			std::thread::sleep(interval);

			if cancelled.load(Ordering::SeqCst) {
				break;
			}

			if actor_ref.send(msg.clone()).is_err() {
				// Actor is dead, stop the timer
				break;
			}
		}
	});

	handle
}
