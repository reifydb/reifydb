// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WASM timer implementation using setTimeout/setInterval.
//!
//! Uses direct global bindings so the timers work in both browser and Node.js
//! environments (web_sys::window() only works in browsers).
//!
//! Timer handles are stored as `JsValue` because browsers return numeric IDs
//! while Node.js returns `Timeout` objects.

use std::{cell::RefCell, rc::Rc, sync::atomic::Ordering, time::Duration};

use js_sys::Function;
use wasm_bindgen::prelude::*;

use super::{TimerHandle, next_timer_id};
use crate::actor::mailbox::ActorRef;

// Global timer functions available in both browser and Node.js.
// Return JsValue because browsers return numbers but Node.js returns Timeout objects.
#[wasm_bindgen]
extern "C" {
	#[wasm_bindgen(js_name = setTimeout)]
	fn global_set_timeout(handler: &Function, timeout: i32) -> JsValue;

	#[wasm_bindgen(js_name = setInterval)]
	fn global_set_interval(handler: &Function, timeout: i32) -> JsValue;

	#[wasm_bindgen(js_name = clearInterval)]
	fn global_clear_interval(handle: &JsValue);
}

/// Schedule a message to be sent after a delay.
///
/// Returns a handle that can be used to cancel the timer.
pub fn schedule_once_fn<M: Send + 'static, F: FnOnce() -> M + Send + 'static>(
	actor_ref: ActorRef<M>,
	delay: Duration,
	factory: F,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();
	let delay_ms = delay.as_millis() as i32;

	let closure = Closure::once(Box::new(move || {
		if !cancelled.load(Ordering::SeqCst) {
			let _ = actor_ref.send(factory());
		}
	}) as Box<dyn FnOnce()>);

	global_set_timeout(closure.as_ref().unchecked_ref(), delay_ms);

	closure.forget();

	handle
}

/// Schedule a message to be sent repeatedly at an interval.
///
/// Uses a factory function to create the message, so `M` doesn't need to be `Clone`.
/// Returns a handle that can be used to cancel the timer.
pub fn schedule_repeat_fn<M: Send + 'static, F: Fn() -> M + Send + 'static>(
	actor_ref: ActorRef<M>,
	interval: Duration,
	factory: F,
) -> TimerHandle {
	let handle = TimerHandle::new(next_timer_id());
	let cancelled = handle.cancelled_flag();

	// Store the interval handle so we can clear it.
	let interval_handle: Rc<RefCell<Option<JsValue>>> = Rc::new(RefCell::new(None));
	let interval_handle_clone = interval_handle.clone();

	let closure = Closure::new(Box::new(move || {
		if cancelled.load(Ordering::SeqCst) {
			if let Some(h) = interval_handle_clone.borrow().as_ref() {
				global_clear_interval(h);
			}
			return;
		}

		if actor_ref.send(factory()).is_err() {
			if let Some(h) = interval_handle_clone.borrow().as_ref() {
				global_clear_interval(h);
			}
		}
	}) as Box<dyn FnMut()>);

	let h = global_set_interval(closure.as_ref().unchecked_ref(), interval.as_millis() as i32);

	*interval_handle.borrow_mut() = Some(h);

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

	// Store the interval handle so we can clear it.
	let interval_handle: Rc<RefCell<Option<JsValue>>> = Rc::new(RefCell::new(None));
	let interval_handle_clone = interval_handle.clone();

	let closure = Closure::new(Box::new(move || {
		if cancelled.load(Ordering::SeqCst) {
			if let Some(h) = interval_handle_clone.borrow().as_ref() {
				global_clear_interval(h);
			}
			return;
		}

		if actor_ref.send(msg.clone()).is_err() {
			if let Some(h) = interval_handle_clone.borrow().as_ref() {
				global_clear_interval(h);
			}
		}
	}) as Box<dyn FnMut()>);

	let h = global_set_interval(closure.as_ref().unchecked_ref(), interval.as_millis() as i32);

	*interval_handle.borrow_mut() = Some(h);

	// Prevent closure from being dropped
	closure.forget();

	handle
}
