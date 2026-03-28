// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Timer utilities for actors.
//!
//! This module provides timer functionality for scheduling messages:
//! - [`TimerHandle`]: A handle to cancel a scheduled timer
//! - [`Context::schedule_once`]: Schedule a message to be sent after a delay
//! - [`Context::schedule_repeat`]: Schedule a message to be sent repeatedly
//!
//! # Platform Differences
//!
//! - **Native**: Uses a centralized scheduler with a BinaryHeap min-heap
//! - **WASM**: Uses `setTimeout` and `setInterval` via `web-sys`
//!
//! [`Context::schedule_once`]: crate::actor::context::Context::schedule_once
//! [`Context::schedule_repeat`]: crate::actor::context::Context::schedule_repeat

use std::{
	fmt,
	fmt::Debug,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

#[cfg(reifydb_target = "native")]
pub mod scheduler;
#[cfg(reifydb_target = "wasi")]
pub(crate) mod wasi;
#[cfg(reifydb_target = "wasm")]
pub(crate) mod wasm;

#[cfg(reifydb_target = "wasi")]
use wasi::drain_expired_timers as wasi_drain;

/// Handle to a scheduled timer.
///
/// Can be used to cancel the timer before it fires.
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

	/// Cancel this timer.
	///
	/// If the timer hasn't fired yet, it will be cancelled.
	/// Returns `true` if the timer was successfully cancelled.
	pub fn cancel(&self) -> bool {
		self.cancelled.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok()
	}

	/// Check if this timer has been cancelled.
	pub fn is_cancelled(&self) -> bool {
		self.cancelled.load(Ordering::SeqCst)
	}

	/// Get the timer ID.
	pub fn id(&self) -> u64 {
		self.id
	}

	/// Get a clone of the cancelled flag.
	pub(crate) fn cancelled_flag(&self) -> Arc<AtomicBool> {
		self.cancelled.clone()
	}
}

impl Debug for TimerHandle {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("TimerHandle").field("id", &self.id).field("cancelled", &self.is_cancelled()).finish()
	}
}

/// Counter for generating unique timer IDs.
static TIMER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn next_timer_id() -> u64 {
	TIMER_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Drain expired timers, firing their callbacks synchronously.
///
/// Only meaningful on WASI where timers are queue-based. No-op on native
/// (thread-based timers) and WASM (JavaScript event loop handles timers).
#[cfg(reifydb_target = "wasi")]
pub fn drain_expired_timers() {
	wasi_drain();
}

/// Drain expired timers (no-op on this platform).
#[cfg(not(reifydb_target = "wasi"))]
pub fn drain_expired_timers() {}
