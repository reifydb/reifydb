// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{thread, time::Instant};

use reifydb_value::value::duration::Duration;

pub fn poll_until<T>(mut f: impl FnMut() -> Option<T>, timeout: Duration) -> Option<T> {
	let deadline = Instant::now() + timeout.to_std();
	loop {
		if let Some(value) = f() {
			return Some(value);
		}
		if Instant::now() >= deadline {
			return None;
		}
		thread::sleep(Duration::from_milliseconds(10).unwrap().to_std());
	}
}
