// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use crate::snapshot::GaugeSnapshot;

/// A value that can go up and down, stored as f64.
///
/// Safe to use from any thread — backed by a single `AtomicU64` storing
/// `f64` bits. Constructable in `const` context so it can live in a `static`.
pub struct Gauge {
	pub name: &'static str,
	pub help: &'static str,
	value: AtomicU64, // stores f64::to_bits()
}

impl Gauge {
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
	pub fn dec(&self) {
		self.add(-1.0);
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
	pub fn set(&self, val: f64) {
		self.value.store(val.to_bits(), Ordering::Relaxed);
	}

	#[inline]
	#[must_use]
	pub fn get(&self) -> f64 {
		f64::from_bits(self.value.load(Ordering::Relaxed))
	}

	#[must_use]
	pub fn snapshot(&self) -> GaugeSnapshot {
		GaugeSnapshot {
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
		let g = Gauge::new("t", "h");
		assert_eq!(g.get(), 0.0);
	}

	#[test]
	fn inc_and_dec() {
		let g = Gauge::new("t", "h");
		g.inc();
		g.inc();
		g.dec();
		assert_eq!(g.get(), 1.0);
	}

	#[test]
	fn set_overwrites() {
		let g = Gauge::new("t", "h");
		g.inc();
		g.set(99.5);
		assert_eq!(g.get(), 99.5);
	}

	#[test]
	fn negative_values() {
		let g = Gauge::new("t", "h");
		g.dec();
		g.dec();
		assert_eq!(g.get(), -2.0);
	}

	#[test]
	fn add_fractional() {
		let g = Gauge::new("t", "h");
		g.add(0.1);
		g.add(0.2);
		assert!((g.get() - 0.3).abs() < 1e-10);
	}

	#[test]
	fn snapshot_reflects_value() {
		let g = Gauge::new("n", "h");
		g.set(-5.0);
		let snap = g.snapshot();
		assert_eq!(snap.name, "n");
		assert_eq!(snap.value, -5.0);
	}
}
