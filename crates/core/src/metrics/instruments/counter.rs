// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::{report::MetricsReporter, sample::MetricsSample};

pub struct Counter {
	pub name: &'static str,
	pub help: &'static str,
	value: AtomicU64,
}

impl Counter {
	pub const fn new(name: &'static str, help: &'static str) -> Self {
		Self {
			name,
			help,
			value: AtomicU64::new(0),
		}
	}

	#[inline]
	pub fn inc(&self) {
		self.add(1.0);
	}

	#[inline]
	pub fn add(&self, n: f64) {
		let mut current = self.value.load(Ordering::Relaxed);
		loop {
			let new = f64::from_bits(current) + n;
			match self.value.compare_exchange_weak(
				current,
				new.to_bits(),
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(actual) => current = actual,
			}
		}
	}

	#[inline]
	#[must_use]
	pub fn get(&self) -> f64 {
		f64::from_bits(self.value.load(Ordering::Relaxed))
	}
}

impl MetricsReporter for Counter {
	fn read(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::count(self.name, "value", self.get() as u64));
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::count::Count;

	use super::*;
	use crate::metrics::sample::Reading;

	#[test]
	fn starts_at_zero() {
		let c = Counter::new("t", "h");
		assert_eq!(c.get(), 0.0);
	}

	#[test]
	fn inc_adds_one() {
		let c = Counter::new("t", "h");
		c.inc();
		c.inc();
		c.inc();
		assert_eq!(c.get(), 3.0);
	}

	#[test]
	fn add_fractional() {
		let c = Counter::new("t", "h");
		c.add(10.5);
		c.add(0.5);
		assert_eq!(c.get(), 11.0);
	}

	#[test]
	fn read_emits_one_count_sample_under_the_instrument_name() {
		// A counter exports one uniform sample; nothing downstream knows counter-specific
		// shapes.
		let c = Counter::new("profiler.accumulator.evictions_total", "h");
		c.add(42.0);
		let mut out = Vec::new();
		c.read(&mut out);
		assert_eq!(out.len(), 1);
		assert_eq!(out[0].scope, "profiler.accumulator.evictions_total");
		assert_eq!(out[0].metric, "value");
		assert_eq!(out[0].reading, Reading::Count(Count::new(42)));
	}
}
