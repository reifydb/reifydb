// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_value::value::duration::Duration;

pub struct StatementMetricsAggregate {
	normalized_rql: String,
	calls: AtomicU64,
	total_duration: AtomicU64,
	max_duration: AtomicU64,
	min_duration: AtomicU64,
	total_compute: AtomicU64,
	total_rows: AtomicU64,
	errors: AtomicU64,
}

impl StatementMetricsAggregate {
	pub fn new(normalized_rql: String) -> Self {
		Self {
			normalized_rql,
			calls: AtomicU64::new(0),
			total_duration: AtomicU64::new(0),
			max_duration: AtomicU64::new(0),
			min_duration: AtomicU64::new(u64::MAX),
			total_compute: AtomicU64::new(0),
			total_rows: AtomicU64::new(0),
			errors: AtomicU64::new(0),
		}
	}

	pub fn record(&self, duration: Duration, compute: Duration, rows: u64, success: bool) {
		let duration_micros = duration.microseconds().unwrap_or(0) as u64;
		let compute_micros = compute.microseconds().unwrap_or(0) as u64;

		self.calls.fetch_add(1, Ordering::Relaxed);
		self.total_duration.fetch_add(duration_micros, Ordering::Relaxed);
		self.total_compute.fetch_add(compute_micros, Ordering::Relaxed);
		self.total_rows.fetch_add(rows, Ordering::Relaxed);

		if !success {
			self.errors.fetch_add(1, Ordering::Relaxed);
		}

		let mut current = self.max_duration.load(Ordering::Relaxed);
		while duration_micros > current {
			match self.max_duration.compare_exchange_weak(
				current,
				duration_micros,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(actual) => current = actual,
			}
		}

		let mut current = self.min_duration.load(Ordering::Relaxed);
		while duration_micros < current {
			match self.min_duration.compare_exchange_weak(
				current,
				duration_micros,
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

	pub fn total_duration(&self) -> Duration {
		Duration::from_micros_infallible(self.total_duration.load(Ordering::Relaxed))
	}

	pub fn max_duration(&self) -> Duration {
		Duration::from_micros_infallible(self.max_duration.load(Ordering::Relaxed))
	}

	pub fn min_duration(&self) -> Duration {
		Duration::from_micros_infallible(self.min_duration.load(Ordering::Relaxed))
	}

	pub fn total_compute(&self) -> Duration {
		Duration::from_micros_infallible(self.total_compute.load(Ordering::Relaxed))
	}

	pub fn total_rows(&self) -> u64 {
		self.total_rows.load(Ordering::Relaxed)
	}

	pub fn errors(&self) -> u64 {
		self.errors.load(Ordering::Relaxed)
	}

	#[must_use]
	pub fn mean_duration(&self) -> Duration {
		let calls = self.calls();
		if calls == 0 {
			Duration::zero()
		} else {
			Duration::from_micros_infallible(self.total_duration.load(Ordering::Relaxed) / calls)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn micros(n: u64) -> Duration {
		Duration::from_micros_infallible(n)
	}

	#[test]
	fn new_stats_defaults() {
		let s = StatementMetricsAggregate::new("SELECT 1".into());
		assert_eq!(s.calls(), 0);
		assert_eq!(s.total_duration(), Duration::zero());
		assert_eq!(s.max_duration(), Duration::zero());
		assert_eq!(s.min_duration(), micros(u64::MAX));
		assert_eq!(s.errors(), 0);
		assert_eq!(s.normalized_rql(), "SELECT 1");
	}

	#[test]
	fn record_updates_all_fields() {
		let s = StatementMetricsAggregate::new("q".into());
		s.record(micros(100), micros(50), 10, true);
		assert_eq!(s.calls(), 1);
		assert_eq!(s.total_duration(), micros(100));
		assert_eq!(s.total_compute(), micros(50));
		assert_eq!(s.total_rows(), 10);
		assert_eq!(s.max_duration(), micros(100));
		assert_eq!(s.min_duration(), micros(100));
		assert_eq!(s.errors(), 0);
	}

	#[test]
	fn min_max_tracking() {
		let s = StatementMetricsAggregate::new("q".into());
		s.record(micros(100), Duration::zero(), 0, true);
		s.record(micros(50), Duration::zero(), 0, true);
		s.record(micros(200), Duration::zero(), 0, true);
		assert_eq!(s.min_duration(), micros(50));
		assert_eq!(s.max_duration(), micros(200));
	}

	#[test]
	fn error_counting() {
		let s = StatementMetricsAggregate::new("q".into());
		s.record(micros(10), Duration::zero(), 0, true);
		s.record(micros(10), Duration::zero(), 0, false);
		s.record(micros(10), Duration::zero(), 0, false);
		assert_eq!(s.calls(), 3);
		assert_eq!(s.errors(), 2);
	}

	#[test]
	fn mean_duration() {
		let s = StatementMetricsAggregate::new("q".into());
		s.record(micros(100), Duration::zero(), 0, true);
		s.record(micros(200), Duration::zero(), 0, true);
		assert_eq!(s.mean_duration(), micros(150));
	}

	#[test]
	fn mean_duration_zero_calls() {
		let s = StatementMetricsAggregate::new("q".into());
		assert_eq!(s.mean_duration(), Duration::zero());
	}
}
