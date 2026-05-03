// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![allow(dead_code)]

use std::{
	alloc::{GlobalAlloc, Layout, System},
	cell::Cell,
	sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let ptr = unsafe { System.alloc(layout) };
		if !ptr.is_null() && counting_active() {
			STATS.allocs.fetch_add(1, Ordering::Relaxed);
			STATS.bytes.fetch_add(layout.size() as u64, Ordering::Relaxed);
		}
		ptr
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { System.dealloc(ptr, layout) };
		if counting_active() {
			STATS.frees.fetch_add(1, Ordering::Relaxed);
			STATS.freed_bytes.fetch_add(layout.size() as u64, Ordering::Relaxed);
		}
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		let ptr = unsafe { System.alloc_zeroed(layout) };
		if !ptr.is_null() && counting_active() {
			STATS.allocs.fetch_add(1, Ordering::Relaxed);
			STATS.bytes.fetch_add(layout.size() as u64, Ordering::Relaxed);
		}
		ptr
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
		if !new_ptr.is_null() && counting_active() {
			let old_size = layout.size();
			if new_size > old_size {
				STATS.bytes.fetch_add((new_size - old_size) as u64, Ordering::Relaxed);
			} else {
				STATS.freed_bytes.fetch_add((old_size - new_size) as u64, Ordering::Relaxed);
			}
			STATS.reallocs.fetch_add(1, Ordering::Relaxed);
		}
		new_ptr
	}
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

static COUNTING: AtomicBool = AtomicBool::new(false);

thread_local! {



	static COUNTING_DEPTH: Cell<u32> = const { Cell::new(0) };
}

fn counting_active() -> bool {
	if !COUNTING.load(Ordering::Relaxed) {
		return false;
	}
	COUNTING_DEPTH.with(|d| d.get() > 0)
}

#[derive(Default)]
struct Stats {
	allocs: AtomicU64,
	frees: AtomicU64,
	reallocs: AtomicU64,
	bytes: AtomicU64,
	freed_bytes: AtomicU64,
}

static STATS: Stats = Stats {
	allocs: AtomicU64::new(0),
	frees: AtomicU64::new(0),
	reallocs: AtomicU64::new(0),
	bytes: AtomicU64::new(0),
	freed_bytes: AtomicU64::new(0),
};

#[derive(Debug, Clone, Copy)]
pub struct Counts {
	pub allocs: u64,
	pub frees: u64,
	pub reallocs: u64,
	pub bytes_allocated: u64,
	pub bytes_freed: u64,
}

impl Counts {
	pub fn net_bytes(&self) -> i64 {
		self.bytes_allocated as i64 - self.bytes_freed as i64
	}
}

pub fn with_counting<R>(f: impl FnOnce() -> R) -> (R, Counts) {
	COUNTING.store(true, Ordering::Relaxed);
	COUNTING_DEPTH.with(|d| d.set(d.get() + 1));

	let allocs0 = STATS.allocs.load(Ordering::Relaxed);
	let frees0 = STATS.frees.load(Ordering::Relaxed);
	let reallocs0 = STATS.reallocs.load(Ordering::Relaxed);
	let bytes0 = STATS.bytes.load(Ordering::Relaxed);
	let freed0 = STATS.freed_bytes.load(Ordering::Relaxed);

	let r = f();

	let counts = Counts {
		allocs: STATS.allocs.load(Ordering::Relaxed) - allocs0,
		frees: STATS.frees.load(Ordering::Relaxed) - frees0,
		reallocs: STATS.reallocs.load(Ordering::Relaxed) - reallocs0,
		bytes_allocated: STATS.bytes.load(Ordering::Relaxed) - bytes0,
		bytes_freed: STATS.freed_bytes.load(Ordering::Relaxed) - freed0,
	};

	COUNTING_DEPTH.with(|d| d.set(d.get() - 1));
	if COUNTING_DEPTH.with(|d| d.get()) == 0 {
		COUNTING.store(false, Ordering::Relaxed);
	}
	(r, counts)
}
