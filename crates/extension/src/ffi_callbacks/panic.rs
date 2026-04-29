// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Panic-catch wrapper for FFI vtable calls.

use std::{
	any::Any,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use tracing::error;

/// Run `f` (an unsafe FFI vtable call) under `catch_unwind`. On panic, log
/// `label` plus the panic payload and `abort()` - panics across the FFI
/// boundary are undefined behaviour, so the host has no safe way to recover.
pub fn call_with_abort_on_panic<F>(label: &str, f: F) -> i32
where
	F: FnOnce() -> i32,
{
	match catch_unwind(AssertUnwindSafe(f)) {
		Ok(code) => code,
		Err(panic_info) => {
			let msg = panic_message(&panic_info);
			error!("FFI {} panicked: {}", label, msg);
			abort();
		}
	}
}

fn panic_message(panic_info: &Box<dyn Any + Send>) -> String {
	if let Some(s) = panic_info.downcast_ref::<&str>() {
		s.to_string()
	} else if let Some(s) = panic_info.downcast_ref::<String>() {
		s.clone()
	} else {
		"Unknown panic".to_string()
	}
}
