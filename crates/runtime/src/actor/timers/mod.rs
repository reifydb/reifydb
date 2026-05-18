// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
