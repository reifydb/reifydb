// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the wg project (https://github.com/al8n/wg),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::sync::{
	Arc,
	atomic::{AtomicUsize, Ordering},
};

#[cfg(feature = "native")]
use reifydb_runtime::sync::condvar::native::Condvar;
#[cfg(feature = "wasm")]
use reifydb_runtime::sync::condvar::wasm::Condvar;

#[cfg(feature = "native")]
use reifydb_runtime::sync::mutex::native::Mutex;
#[cfg(feature = "wasm")]
use reifydb_runtime::sync::mutex::wasm::Mutex;

struct Inner {
	count: AtomicUsize,
	mutex: Mutex<()>,
	condvar: Condvar,
}

pub struct WaitGroup {
	inner: Arc<Inner>,
}

impl Default for WaitGroup {
	fn default() -> Self {
		Self::new()
	}
}

impl From<usize> for WaitGroup {
	fn from(count: usize) -> Self {
		Self {
			inner: Arc::new(Inner {
				count: AtomicUsize::new(count),
				mutex: Mutex::new(()),
				condvar: Condvar::new(),
			}),
		}
	}
}

impl Clone for WaitGroup {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl std::fmt::Debug for WaitGroup {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let count = self.inner.count.load(Ordering::Acquire);
		f.debug_struct("WaitGroup").field("count", &count).finish()
	}
}

impl WaitGroup {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(Inner {
				count: AtomicUsize::new(0),
				mutex: Mutex::new(()),
				condvar: Condvar::new(),
			}),
		}
	}

	pub fn add(&self, num: usize) -> Self {
		self.inner.count.fetch_add(num, Ordering::AcqRel);
		Self {
			inner: self.inner.clone(),
		}
	}

	pub fn done(&self) -> usize {
		let prev = self.inner.count.fetch_sub(1, Ordering::AcqRel);
		if prev == 1 {
			let _guard = self.inner.mutex.lock();
			self.inner.condvar.notify_all();
		}
		if prev == 0 {
			// Already at zero, restore it (shouldn't happen in correct usage)
			self.inner.count.fetch_add(1, Ordering::AcqRel);
			return 0;
		}
		prev - 1
	}

	pub fn waitings(&self) -> usize {
		self.inner.count.load(Ordering::Acquire)
	}

	pub fn wait(&self) {
		let mut guard = self.inner.mutex.lock();
		while self.inner.count.load(Ordering::Acquire) > 0 {
			self.inner.condvar.wait(&mut guard);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use std::{
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
		time::Duration,
	};

	use crate::util::wait_group::WaitGroup;

	#[test]
	fn test_wait_group_reuse() {
		let wg = WaitGroup::new();
		let ctr = Arc::new(AtomicUsize::new(0));
		for _ in 0..6 {
			let wg = wg.add(1);
			let ctrx = ctr.clone();
			std::thread::spawn(move || {
				std::thread::sleep(Duration::from_millis(5));
				ctrx.fetch_add(1, Ordering::Relaxed);
				wg.done();
			});
		}

		wg.wait();
		assert_eq!(ctr.load(Ordering::Relaxed), 6);

		let worker = wg.add(1);
		let ctrx = ctr.clone();
		std::thread::spawn(move || {
			std::thread::sleep(Duration::from_millis(5));
			ctrx.fetch_add(1, Ordering::Relaxed);
			worker.done();
		});
		wg.wait();
		assert_eq!(ctr.load(Ordering::Relaxed), 7);
	}

	#[test]
	fn test_wait_group_nested() {
		let wg = WaitGroup::new();
		let ctr = Arc::new(AtomicUsize::new(0));
		for _ in 0..5 {
			let worker = wg.add(1);
			let ctrx = ctr.clone();
			std::thread::spawn(move || {
				let nested_worker = worker.add(1);
				let ctrxx = ctrx.clone();
				std::thread::spawn(move || {
					ctrxx.fetch_add(1, Ordering::Relaxed);
					nested_worker.done();
				});
				ctrx.fetch_add(1, Ordering::Relaxed);
				worker.done();
			});
		}

		wg.wait();
		assert_eq!(ctr.load(Ordering::Relaxed), 10);
	}

	#[test]
	fn test_wait_group_from() {
		let wg = WaitGroup::from(5);
		for _ in 0..5 {
			let t = wg.clone();
			std::thread::spawn(move || {
				t.done();
			});
		}
		wg.wait();
	}

	#[test]
	fn test_clone_and_fmt() {
		let swg = WaitGroup::new();
		let swg1 = swg.clone();
		swg1.add(3);
		assert_eq!(format!("{:?}", swg), format!("{:?}", swg1));
	}

	#[test]
	fn test_waitings() {
		let wg = WaitGroup::new();
		wg.add(1);
		wg.add(1);
		assert_eq!(wg.waitings(), 2);
	}
}
