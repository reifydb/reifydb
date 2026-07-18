// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

#[cfg(not(target_env = "msvc"))]
use tikv_jemalloc_ctl::{
	epoch, stats,
	stats_print::{self, Options},
};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
pub type Allocator = Jemalloc;

#[cfg(not(target_env = "msvc"))]
pub const ALLOCATOR: Allocator = Jemalloc;

#[cfg(not(target_env = "msvc"))]
pub fn verify() {
	epoch::advance().expect(
		"reifydb-allocator-jemalloc: jemalloc epoch advance failed; jemalloc is not the active allocator for this process",
	);
	let allocated = stats::allocated::read()
		.expect("reifydb-allocator-jemalloc: jemalloc stats::allocated read failed; jemalloc is not the active allocator for this process");
	assert!(
		allocated > 0,
		"reifydb-allocator-jemalloc: jemalloc reports 0 allocated bytes at startup; allocator is not bound to this process"
	);
}

#[cfg(not(target_env = "msvc"))]
pub fn stats() -> (u64, u64, u64, u64, u64, u64) {
	let _ = epoch::advance();
	(
		stats::allocated::read().unwrap_or(0) as u64,
		stats::active::read().unwrap_or(0) as u64,
		stats::resident::read().unwrap_or(0) as u64,
		stats::mapped::read().unwrap_or(0) as u64,
		stats::retained::read().unwrap_or(0) as u64,
		stats::metadata::read().unwrap_or(0) as u64,
	)
}

#[cfg(not(target_env = "msvc"))]
pub fn stats_dump() -> Option<String> {
	let _ = epoch::advance();
	let mut buf = Vec::new();
	stats_print::stats_print(&mut buf, Options::default()).ok()?;
	String::from_utf8(buf).ok()
}

#[cfg(target_env = "msvc")]
compile_error!("reifydb-allocator-jemalloc does not support MSVC; use the (Phase 2) mimalloc backend instead");
