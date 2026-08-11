// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! MOKA_LRU_ENTRY_OVERHEAD was derived from this measurement (moka 0.12.15, LRU, no
//! TTL), so this is the tripwire when a moka upgrade changes its per-entry structures. The file
//! holds exactly one test, or concurrent test threads pollute the live-bytes counter.
#![cfg(not(reifydb_single_threaded))]

use std::{
	alloc::{GlobalAlloc, Layout, System},
	mem::size_of,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

use reifydb_runtime::cache::sync::{CacheFootprint, SyncLru};

struct CountingAllocator;

static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every method forwards its caller's layout and pointer contract unchanged to the system
// allocator, so allocation validity, alignment and ownership are exactly System's; the counter is
// an atomic side effect that touches no allocated memory.
unsafe impl GlobalAlloc for CountingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		LIVE.fetch_add(layout.size(), Ordering::Relaxed);
		// SAFETY: layout comes from the caller and is forwarded unchanged.
		unsafe { System.alloc(layout) }
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
		// SAFETY: ptr was allocated by System with this layout and is not used again.
		unsafe { System.dealloc(ptr, layout) }
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		LIVE.fetch_add(new_size, Ordering::Relaxed);
		LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
		// SAFETY: ptr was allocated by System with this layout and new_size is the caller's.
		unsafe { System.realloc(ptr, layout, new_size) }
	}
}

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn pubkey_footprint(_key: &[u8; 32], value: &Arc<str>) -> CacheFootprint {
	// Mirrors the pubkey cache, the largest SyncLru user, so the calibration matches real use.
	CacheFootprint {
		heap: 2 * size_of::<usize>() + value.len(),
		payload: 32 + value.len(),
	}
}

const ENTRIES: usize = 50_000;
const VALUE: &str = "2vjK1eZZ4DDo1XABkI9AhTPGkZ30xhH39CzHfMTgRMkE";

#[test]
fn resident_tracks_the_real_allocator_within_five_percent() {
	let cache: SyncLru<[u8; 32], Arc<str>> = SyncLru::measured(4_000_000, pubkey_footprint);
	cache.run_pending_tasks();

	let before = LIVE.load(Ordering::Relaxed);
	for i in 0..ENTRIES {
		let mut key = [0u8; 32];
		key[..8].copy_from_slice(&(i as u64).to_le_bytes());
		cache.put(key, Arc::from(VALUE));
	}
	cache.run_pending_tasks();
	let allocated = (LIVE.load(Ordering::Relaxed) - before) as f64;

	let usage = cache.memory_usage().expect("measured cache must report usage");
	assert_eq!(usage.entries.as_u64(), ENTRIES as u64);
	assert_eq!(usage.payload.as_bytes(), (ENTRIES * (32 + VALUE.len())) as u64);

	let reported = usage.resident.as_bytes() as f64;
	let deviation = (reported - allocated).abs() / allocated;
	assert!(
		deviation < 0.05,
		"reported resident {} vs actually allocated {} deviates {:.1}% (>5%): \
		 MOKA_LRU_ENTRY_OVERHEAD no longer matches moka's real per-entry cost",
		reported,
		allocated,
		deviation * 100.0
	);
}
