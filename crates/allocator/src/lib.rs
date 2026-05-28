// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

#[cfg(all(feature = "alloc-jemalloc", not(target_env = "msvc")))]
pub mod backend {
	use reifydb_allocator_jemalloc::{ALLOCATOR as JEMALLOC_ALLOCATOR, verify as jemalloc_verify};

	pub type Allocator = reifydb_allocator_jemalloc::Allocator;
	pub const ALLOCATOR: Allocator = JEMALLOC_ALLOCATOR;

	pub fn verify() {
		jemalloc_verify();
	}
}

#[cfg(all(feature = "alloc-jemalloc", target_env = "msvc"))]
compile_error!(
	"reifydb-allocator: jemalloc is not supported on MSVC. \
	 Use the (Phase 2) `alloc-mimalloc` feature instead. \
	 See reifydb/plan-alloc.md."
);

#[macro_export]
#[cfg(feature = "alloc-jemalloc")]
macro_rules! set_global_allocator {
	() => {
		#[global_allocator]
		static REIFYDB_GLOBAL_ALLOCATOR: $crate::backend::Allocator = $crate::backend::ALLOCATOR;
	};
}

#[macro_export]
#[cfg(not(feature = "alloc-jemalloc"))]
macro_rules! set_global_allocator {
	() => {};
}

#[cfg(feature = "alloc-jemalloc")]
pub fn verify() {
	backend::verify();
}

#[cfg(not(feature = "alloc-jemalloc"))]
pub fn verify() {}
