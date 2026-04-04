// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-fingerprint query statistics with all-atomic fields.
///
/// One instance exists per unique statement fingerprint in the accumulator.
/// All mutations are lock-free after initial insertion.
pub struct StatementStats {
	normalized_rql: String,
	calls: AtomicU64,
	total_duration_us: AtomicU64,
	max_duration_us: AtomicU64,
	min_duration_us: AtomicU64,
	total_compute_us: AtomicU64,
	total_rows: AtomicU64,
	errors: AtomicU64,
}

impl StatementStats {
	pub fn new(normalized_rql: String) -> Self {
		Self {
			normalized_rql,
			calls: AtomicU64::new(0),
			total_duration_us: AtomicU64::new(0),
			max_duration_us: AtomicU64::new(0),
			min_duration_us: AtomicU64::new(u64::MAX),
			total_compute_us: AtomicU64::new(0),
			total_rows: AtomicU64::new(0),
			errors: AtomicU64::new(0),
		}
	}

	pub fn record(&self, duration_us: u64, compute_us: u64, rows: u64, success: bool) {
		self.calls.fetch_add(1, Ordering::Relaxed);
		self.total_duration_us.fetch_add(duration_us, Ordering::Relaxed);
		self.total_compute_us.fetch_add(compute_us, Ordering::Relaxed);
		self.total_rows.fetch_add(rows, Ordering::Relaxed);

		if !success {
			self.errors.fetch_add(1, Ordering::Relaxed);
		}

		// CAS loop for max
		let mut current = self.max_duration_us.load(Ordering::Relaxed);
		while duration_us > current {
			match self.max_duration_us.compare_exchange_weak(
				current,
				duration_us,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(actual) => current = actual,
			}
		}

		// CAS loop for min
		let mut current = self.min_duration_us.load(Ordering::Relaxed);
		while duration_us < current {
			match self.min_duration_us.compare_exchange_weak(
				current,
				duration_us,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(actual) => current = actual,
			}
		}
	}

	pub fn normalized_rql(&self) -> &str {
		&self.normalized_rql
	}

	pub fn calls(&self) -> u64 {
		self.calls.load(Ordering::Relaxed)
	}

	pub fn total_duration_us(&self) -> u64 {
		self.total_duration_us.load(Ordering::Relaxed)
	}

	pub fn max_duration_us(&self) -> u64 {
		self.max_duration_us.load(Ordering::Relaxed)
	}

	pub fn min_duration_us(&self) -> u64 {
		self.min_duration_us.load(Ordering::Relaxed)
	}

	pub fn total_compute_us(&self) -> u64 {
		self.total_compute_us.load(Ordering::Relaxed)
	}

	pub fn total_rows(&self) -> u64 {
		self.total_rows.load(Ordering::Relaxed)
	}

	pub fn errors(&self) -> u64 {
		self.errors.load(Ordering::Relaxed)
	}

	#[must_use]
	pub fn mean_duration_us(&self) -> u64 {
		let calls = self.calls();
		if calls == 0 {
			0
		} else {
			self.total_duration_us() / calls
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn new_stats_defaults() {
		let s = StatementStats::new("SELECT 1".into());
		assert_eq!(s.calls(), 0);
		assert_eq!(s.total_duration_us(), 0);
		assert_eq!(s.max_duration_us(), 0);
		assert_eq!(s.min_duration_us(), u64::MAX);
		assert_eq!(s.errors(), 0);
		assert_eq!(s.normalized_rql(), "SELECT 1");
	}

	#[test]
	fn record_updates_all_fields() {
		let s = StatementStats::new("q".into());
		s.record(100, 50, 10, true);
		assert_eq!(s.calls(), 1);
		assert_eq!(s.total_duration_us(), 100);
		assert_eq!(s.total_compute_us(), 50);
		assert_eq!(s.total_rows(), 10);
		assert_eq!(s.max_duration_us(), 100);
		assert_eq!(s.min_duration_us(), 100);
		assert_eq!(s.errors(), 0);
	}

	#[test]
	fn min_max_tracking() {
		let s = StatementStats::new("q".into());
		s.record(100, 0, 0, true);
		s.record(50, 0, 0, true);
		s.record(200, 0, 0, true);
		assert_eq!(s.min_duration_us(), 50);
		assert_eq!(s.max_duration_us(), 200);
	}

	#[test]
	fn error_counting() {
		let s = StatementStats::new("q".into());
		s.record(10, 0, 0, true);
		s.record(10, 0, 0, false);
		s.record(10, 0, 0, false);
		assert_eq!(s.calls(), 3);
		assert_eq!(s.errors(), 2);
	}

	#[test]
	fn mean_duration() {
		let s = StatementStats::new("q".into());
		s.record(100, 0, 0, true);
		s.record(200, 0, 0, true);
		assert_eq!(s.mean_duration_us(), 150);
	}

	#[test]
	fn mean_duration_zero_calls() {
		let s = StatementStats::new("q".into());
		assert_eq!(s.mean_duration_us(), 0);
	}
}
