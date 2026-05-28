// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

#[cfg(not(target_env = "msvc"))]
use tikv_jemalloc_ctl::{epoch, stats};
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

#[cfg(target_env = "msvc")]
compile_error!("reifydb-allocator-jemalloc does not support MSVC; use the (Phase 2) mimalloc backend instead");
