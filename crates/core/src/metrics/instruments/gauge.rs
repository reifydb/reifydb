// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::{
	report::MetricsReporter,
	sample::{MetricsSample, ReadingKind},
};

pub struct Gauge {
	pub name: &'static str,
	pub help: &'static str,
	pub kind: ReadingKind,
	value: AtomicU64,
}

impl Gauge {
	pub const fn new(name: &'static str, help: &'static str, kind: ReadingKind) -> Self {
		Self {
			name,
			help,
			kind,
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
}

impl MetricsReporter for Gauge {
	fn read(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample {
			scope: self.name.into(),
			metric: "value",
			reading: self.kind.reading(self.get()),
		});
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::byte_size::ByteSize;

	use super::*;
	use crate::metrics::sample::Reading;

	#[test]
	fn starts_at_zero() {
		let g = Gauge::new("t", "h", ReadingKind::Count);
		assert_eq!(g.get(), 0.0);
	}

	#[test]
	fn inc_and_dec() {
		let g = Gauge::new("t", "h", ReadingKind::Count);
		g.inc();
		g.inc();
		g.dec();
		assert_eq!(g.get(), 1.0);
	}

	#[test]
	fn set_overwrites() {
		let g = Gauge::new("t", "h", ReadingKind::Count);
		g.inc();
		g.set(99.5);
		assert_eq!(g.get(), 99.5);
	}

	#[test]
	fn negative_values() {
		let g = Gauge::new("t", "h", ReadingKind::Count);
		g.dec();
		g.dec();
		assert_eq!(g.get(), -2.0);
	}

	#[test]
	fn add_fractional() {
		let g = Gauge::new("t", "h", ReadingKind::Count);
		g.add(0.1);
		g.add(0.2);
		assert!((g.get() - 0.3).abs() < 1e-10);
	}

	#[test]
	fn read_emits_the_declared_reading_kind() {
		// The declared ReadingKind types the exported value; instruments never count as
		// named heap.
		let g = Gauge::new("cache.bytes", "h", ReadingKind::Bytes);
		g.set(4096.0);
		let mut out = Vec::new();
		g.read(&mut out);
		assert_eq!(out.len(), 1);
		assert_eq!(out[0].scope, "cache.bytes");
		assert_eq!(out[0].reading, Reading::Bytes(ByteSize::from_bytes(4096)));
		assert_eq!(out[0].reading.heap_bytes(), None, "instrument readings must never count as named heap");
	}
}
