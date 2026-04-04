// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use crate::snapshot::CounterSnapshot;

/// A monotonically increasing counter with f64 values.
///
/// Safe to use from any thread — backed by a single `AtomicU64` storing
/// `f64` bits. Constructable in `const` context so it can live in a `static`.
pub struct Counter {
	pub name: &'static str,
	pub help: &'static str,
	value: AtomicU64, // stores f64::to_bits()
}

impl Counter {
	pub const fn new(name: &'static str, help: &'static str) -> Self {
		// 0u64 == f64::to_bits(0.0)
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

	#[must_use]
	pub fn snapshot(&self) -> CounterSnapshot {
		CounterSnapshot {
			name: self.name,
			help: self.help,
			value: self.get(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

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
	fn snapshot_reflects_value() {
		let c = Counter::new("n", "h");
		c.add(42.0);
		let snap = c.snapshot();
		assert_eq!(snap.name, "n");
		assert_eq!(snap.value, 42.0);
	}
}
