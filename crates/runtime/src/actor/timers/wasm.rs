// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM timer implementation using setTimeout/setInterval.

use std::{cell::RefCell, rc::Rc, sync::atomic::Ordering, time::Duration};

use wasm_bindgen::prelude::*;

use super::{TimerHandle, next_timer_id};
use crate::actor::mailbox::ActorRef;

/// Schedule a message to be sent after a delay.
///
/// Returns a handle that can be used to cancel the timer.
pub fn schedule_once<M: Send + Clone + 'static>(actor_ref: ActorRef<M>, delay: Duration, msg: M) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();
	let delay_ms = delay.as_millis() as i32;

	// In WASM, we use setTimeout via wasm-bindgen
	let closure = Closure::once(Box::new(move || {
		if !cancelled.load(Ordering::SeqCst) {
			let _ = actor_ref.send(msg);
		}
	}) as Box<dyn FnOnce()>);

	let window = web_sys::window().expect("no global `window` exists");
	let _ = window
		.set_timeout_with_callback_and_timeout_and_arguments_0(closure.as_ref().unchecked_ref(), delay_ms);

	// Prevent closure from being dropped
	closure.forget();

	handle
}

/// Schedule a message to be sent repeatedly at an interval.
///
/// Returns a handle that can be used to cancel the timer.
pub fn schedule_repeat<M: Send + Clone + 'static>(actor_ref: ActorRef<M>, interval: Duration, msg: M) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();

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
