// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use tracing::error;

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
