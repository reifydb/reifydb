// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cell::Cell;

thread_local! {
	static FETCHED: Cell<u64> = const { Cell::new(0) };
	static TOMBSTONES: Cell<u64> = const { Cell::new(0) };
}

pub fn record_page(fetched: u64, tombstones: u64) {
	FETCHED.with(|c| c.set(c.get().wrapping_add(fetched)));
	TOMBSTONES.with(|c| c.set(c.get().wrapping_add(tombstones)));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanCounters {
	pub fetched: u64,
	pub tombstones: u64,
}

impl ScanCounters {
	pub fn sample() -> Self {
		Self {
			fetched: FETCHED.with(|c| c.get()),
			tombstones: TOMBSTONES.with(|c| c.get()),
		}
	}

	pub fn since(self) -> Self {
		let now = Self::sample();
		Self {
			fetched: now.fetched.wrapping_sub(self.fetched),
			tombstones: now.tombstones.wrapping_sub(self.tombstones),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn since_reports_only_what_the_caller_bracketed() {
		// The counter is process-lifetime and shared by every scan on this thread, so a call site
		// can only attribute the delta across its own execution. Reading the absolute value would
		// bill each site for every scan that ran before it.
		record_page(100, 90);
		let before = ScanCounters::sample();
		record_page(7, 3);
		let delta = before.since();

		assert_eq!(delta.fetched, 7, "rows fetched before the bracket must not be attributed to it");
		assert_eq!(delta.tombstones, 3);
	}

	#[test]
	fn a_bracket_with_no_scan_reports_nothing() {
		record_page(5, 5);
		let before = ScanCounters::sample();

		assert_eq!(before.since(), ScanCounters {
			fetched: 0,
			tombstones: 0
		});
	}
}
