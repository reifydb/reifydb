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

#[cfg(feature = "native")]
pub(crate) mod native;
#[cfg(feature = "wasm")]
pub(crate) mod wasm;

// =============================================================================
// Shared types
// =============================================================================

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

	/// Get a clone of the cancelled flag.
	pub(crate) fn cancelled_flag(&self) -> Arc<AtomicBool> {
		self.cancelled.clone()
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

pub(crate) fn next_timer_id() -> u64 {
	TIMER_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

// =============================================================================
// Re-exports
// =============================================================================

#[cfg(feature = "native")]
pub use native::{schedule_once, schedule_repeat};

#[cfg(feature = "wasm")]
pub use wasm::{schedule_once, schedule_repeat};
