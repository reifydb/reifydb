// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Timer utilities for actors.
//!
//! This module provides timer functionality for scheduling messages:
//! - [`TimerHandle`]: A handle to cancel a scheduled timer
//! - [`schedule_once`]: Schedule a message to be sent after a delay
//! - [`schedule_repeat`]: Schedule a message to be sent repeatedly
//!
//! # Platform Differences
//!
//! - **Native**: Uses `std::thread` with `std::thread::sleep` for timing
//! - **WASM**: Uses `setTimeout` and `setInterval` via `web-sys`

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::actor::mailbox::ActorRef;

/// Handle to a scheduled timer.
///
/// Can be used to cancel the timer before it fires.
#[derive(Clone)]
pub struct TimerHandle {
	id: u64,
	cancelled: Arc<AtomicBool>,
}

impl TimerHandle {
	fn new(id: u64) -> Self {
		Self {
			id,
			cancelled: Arc::new(AtomicBool::new(false)),
		}
	}

	/// Cancel this timer.
	///
	/// If the timer hasn't fired yet, it will be cancelled.
	/// Returns `true` if the timer was successfully cancelled.
	pub fn cancel(&self) -> bool {
		self.cancelled
			.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
			.is_ok()
	}

	/// Check if this timer has been cancelled.
	pub fn is_cancelled(&self) -> bool {
		self.cancelled.load(Ordering::SeqCst)
	}

	/// Get the timer ID.
	pub fn id(&self) -> u64 {
		self.id
	}
}

impl std::fmt::Debug for TimerHandle {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("TimerHandle")
			.field("id", &self.id)
			.field("cancelled", &self.is_cancelled())
			.finish()
	}
}

/// Counter for generating unique timer IDs.
static TIMER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_timer_id() -> u64 {
	TIMER_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

// =============================================================================
// Native: std::thread based timers
// =============================================================================

/// Schedule a message to be sent after a delay.
///
/// Returns a handle that can be used to cancel the timer.
#[cfg(feature = "native")]
pub fn schedule_once<M: Send + Clone + 'static>(
	actor_ref: ActorRef<M>,
	delay: Duration,
	msg: M,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled.clone();

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
#[cfg(feature = "native")]
pub fn schedule_repeat<M: Send + Clone + 'static>(
	actor_ref: ActorRef<M>,
	interval: Duration,
	msg: M,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled.clone();

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

// =============================================================================
// WASM: setTimeout/setInterval based timers
// =============================================================================

/// Schedule a message to be sent after a delay.
///
/// Returns a handle that can be used to cancel the timer.
#[cfg(feature = "wasm")]
pub fn schedule_once<M: Send + Clone + 'static>(
	actor_ref: ActorRef<M>,
	delay: Duration,
	msg: M,
) -> TimerHandle {
	use wasm_bindgen::prelude::*;

	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled.clone();

	// In WASM, we use setTimeout via wasm-bindgen
	let closure = Closure::once(Box::new(move || {
		if !cancelled.load(Ordering::SeqCst) {
			let _ = actor_ref.send(msg);
		}
	}) as Box<dyn FnOnce()>);

	let window = web_sys::window().expect("no global `window` exists");
	let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
		closure.as_ref().unchecked_ref(),
		delay.as_millis() as i32,
	);

	// Prevent closure from being dropped
	closure.forget();

	handle
}

/// Schedule a message to be sent repeatedly at an interval.
///
/// Returns a handle that can be used to cancel the timer.
#[cfg(feature = "wasm")]
pub fn schedule_repeat<M: Send + Clone + 'static>(
	actor_ref: ActorRef<M>,
	interval: Duration,
	msg: M,
) -> TimerHandle {
	use std::cell::RefCell;
	use std::rc::Rc;
	use wasm_bindgen::prelude::*;

	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled.clone();

	// Store the interval ID so we can clear it
	let interval_id: Rc<RefCell<Option<i32>>> = Rc::new(RefCell::new(None));
	let interval_id_clone = interval_id.clone();

	let closure = Closure::new(Box::new(move || {
		if cancelled.load(Ordering::SeqCst) {
			// Cancel the interval
			if let Some(id) = *interval_id_clone.borrow() {
				let window = web_sys::window().expect("no global `window` exists");
				window.clear_interval_with_handle(id);
			}
			return;
		}

		if actor_ref.send(msg.clone()).is_err() {
			// Actor is dead, cancel the interval
			if let Some(id) = *interval_id_clone.borrow() {
				let window = web_sys::window().expect("no global `window` exists");
				window.clear_interval_with_handle(id);
			}
		}
	}) as Box<dyn FnMut()>);

	let window = web_sys::window().expect("no global `window` exists");
	let id = window
		.set_interval_with_callback_and_timeout_and_arguments_0(
			closure.as_ref().unchecked_ref(),
			interval.as_millis() as i32,
		)
		.expect("failed to set interval");

	*interval_id.borrow_mut() = Some(id);

	// Prevent closure from being dropped
	closure.forget();

	handle
}
