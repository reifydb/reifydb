// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_value::reifydb_assertions;

use crate::metrics::{
	report::MetricsReporter,
	sample::{MetricsSample, ReadingKind},
};

pub struct Histogram {
	pub name: &'static str,
	pub help: &'static str,
	pub kind: ReadingKind,
	pub boundaries: &'static [f64],
	buckets: Vec<AtomicU64>,
	sum: AtomicU64,
	count: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct Percentiles {
	pub p5: f64,
	pub p10: f64,
	pub p15: f64,
	pub p20: f64,
	pub p25: f64,
	pub p30: f64,
	pub p35: f64,
	pub p40: f64,
	pub p45: f64,
	pub p50: f64,
	pub p55: f64,
	pub p60: f64,
	pub p65: f64,
	pub p70: f64,
	pub p75: f64,
	pub p80: f64,
	pub p85: f64,
	pub p90: f64,
	pub p95: f64,
	pub p96: f64,
	pub p97: f64,
	pub p98: f64,
	pub p99: f64,
	pub max: f64,
}

impl Histogram {
	pub fn new(name: &'static str, help: &'static str, kind: ReadingKind, boundaries: &'static [f64]) -> Self {
		let buckets = (0..boundaries.len() + 1).map(|_| AtomicU64::new(0)).collect();
		Self {
			name,
			help,
			kind,
			boundaries,
			buckets,
			sum: AtomicU64::new(0),
			count: AtomicU64::new(0),
		}
	}

	#[inline]
	pub fn observe(&self, value: f64) {
		let idx = self.boundaries.partition_point(|&b| b < value);
		reifydb_assertions! {
			assert!(
				idx < self.buckets.len(),
				"histogram bucket index from partition_point exceeds the bucket array length, which means the histogram was constructed with a mismatched boundaries/buckets pair and any observation silently corrupts adjacent memory (idx={} buckets_len={} boundaries_len={})",
				idx,
				self.buckets.len(),
				self.boundaries.len()
			);
		}
		self.buckets[idx].fetch_add(1, Ordering::Relaxed);

		let mut current = self.sum.load(Ordering::Relaxed);
		loop {
			let new = f64::from_bits(current) + value;
			match self.sum.compare_exchange_weak(
				current,
				new.to_bits(),
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(actual) => current = actual,
			}
		}

		self.count.fetch_add(1, Ordering::Relaxed);
	}

	#[must_use]
	pub fn count(&self) -> u64 {
		self.count.load(Ordering::Relaxed)
	}

	#[must_use]
	pub fn sum(&self) -> f64 {
		f64::from_bits(self.sum.load(Ordering::Relaxed))
	}

	#[must_use]
	pub fn percentiles(&self) -> Percentiles {
		let bucket_counts: Vec<u64> = self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).collect();
		compute_percentiles(&bucket_counts, self.count(), self.boundaries)
	}
}

impl MetricsReporter for Histogram {
	fn read(&self, out: &mut Vec<MetricsSample>) {
		let percentiles = self.percentiles();
		out.push(MetricsSample::count(self.name, "count", self.count()));
		out.push(MetricsSample {
			scope: self.name.into(),
			metric: "sum",
			reading: self.kind.reading(self.sum()),
		});
		out.push(MetricsSample {
			scope: self.name.into(),
			metric: "p50",
			reading: self.kind.reading(percentiles.p50),
		});
		out.push(MetricsSample {
			scope: self.name.into(),
			metric: "p95",
			reading: self.kind.reading(percentiles.p95),
		});
		out.push(MetricsSample {
			scope: self.name.into(),
			metric: "p99",
			reading: self.kind.reading(percentiles.p99),
		});
		out.push(MetricsSample {
			scope: self.name.into(),
			metric: "max",
			reading: self.kind.reading(percentiles.max),
		});
	}
}

const QUANTILES: &[f64] = &[
	0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90,
	0.95, 0.96, 0.97, 0.98, 0.99,
];

fn compute_percentiles(buckets: &[u64], count: u64, boundaries: &[f64]) -> Percentiles {
	if count == 0 {
		return Percentiles::default();
	}

	let targets: Vec<u64> = QUANTILES.iter().map(|&q| (count as f64 * q).ceil() as u64).collect();

	let mut result = [0.0f64; 23];
	let mut found = [false; 23];
	let mut max = 0.0f64;
	let mut cumulative = 0u64;

	for (i, &bucket_count) in buckets.iter().enumerate() {
		if bucket_count > 0 {
			max = if i < boundaries.len() {
				boundaries[i]
			} else {
				f64::INFINITY
			};
		}
		cumulative += bucket_count;

		let bound = if i < boundaries.len() {
			boundaries[i]
		} else {
			f64::INFINITY
		};
		for (j, &target) in targets.iter().enumerate() {
			if !found[j] && cumulative >= target {
				result[j] = bound;
				found[j] = true;
			}
		}
	}

	Percentiles {
		p5: result[0],
		p10: result[1],
		p15: result[2],
		p20: result[3],
		p25: result[4],
		p30: result[5],
		p35: result[6],
		p40: result[7],
		p45: result[8],
		p50: result[9],
		p55: result[10],
		p60: result[11],
		p65: result[12],
		p70: result[13],
		p75: result[14],
		p80: result[15],
		p85: result[16],
		p90: result[17],
		p95: result[18],
		p96: result[19],
		p97: result[20],
		p98: result[21],
		p99: result[22],
		max,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::metrics::sample::Reading;

	static SIMPLE_BOUNDS: &[f64] = &[10.0, 20.0, 50.0, 100.0];

	#[test]
	fn empty_histogram() {
		let h = Histogram::new("t", "h", ReadingKind::Ratio, SIMPLE_BOUNDS);
		assert_eq!(h.count(), 0);
		assert_eq!(h.sum(), 0.0);
		let p = h.percentiles();
		assert_eq!(p.p50, 0.0);
		assert_eq!(p.max, 0.0);
	}

	#[test]
	fn single_observation() {
		let h = Histogram::new("t", "h", ReadingKind::Ratio, SIMPLE_BOUNDS);
		h.observe(15.0);
		assert_eq!(h.count(), 1);
		assert_eq!(h.sum(), 15.0);
		// All percentiles resolve to the 20.0 bucket upper bound
		let p = h.percentiles();
		assert_eq!(p.p5, 20.0);
		assert_eq!(p.p99, 20.0);
		assert_eq!(p.max, 20.0);
	}

	#[test]
	fn below_first_boundary() {
		let h = Histogram::new("t", "h", ReadingKind::Ratio, SIMPLE_BOUNDS);
		h.observe(5.0);
		assert_eq!(h.buckets[0].load(Ordering::Relaxed), 1); // [..10] bucket
		assert_eq!(h.percentiles().p50, 10.0);
	}

	#[test]
	fn above_last_boundary() {
		let h = Histogram::new("t", "h", ReadingKind::Ratio, SIMPLE_BOUNDS);
		h.observe(200.0);
		assert_eq!(h.buckets.last().unwrap().load(Ordering::Relaxed), 1); // overflow bucket
		assert_eq!(h.percentiles().max, f64::INFINITY);
	}

	#[test]
	fn exactly_on_boundary() {
		let h = Histogram::new("t", "h", ReadingKind::Ratio, SIMPLE_BOUNDS);
		// partition_point(|&b| b < 10.0) → first bucket where boundary >= 10.0 → index 0
		// So exactly-on-boundary falls into the bucket whose upper bound is that boundary
		h.observe(10.0);
		assert_eq!(h.buckets[0].load(Ordering::Relaxed), 1);
	}

	#[test]
	fn sum_tracks_f64() {
		let h = Histogram::new("t", "h", ReadingKind::Ratio, SIMPLE_BOUNDS);
		h.observe(1.5);
		h.observe(2.5);
		h.observe(3.0);
		assert_eq!(h.sum(), 7.0);
		assert_eq!(h.count(), 3);
	}

	#[test]
	fn percentiles_with_uniform_distribution() {
		// 100 observations across 100 fine-grained buckets
		static FINE: &[f64] = &[
			1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0,
			18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0,
			34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0,
			50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0, 65.0,
			66.0, 67.0, 68.0, 69.0, 70.0, 71.0, 72.0, 73.0, 74.0, 75.0, 76.0, 77.0, 78.0, 79.0, 80.0, 81.0,
			82.0, 83.0, 84.0, 85.0, 86.0, 87.0, 88.0, 89.0, 90.0, 91.0, 92.0, 93.0, 94.0, 95.0, 96.0, 97.0,
			98.0, 99.0, 100.0,
		];
		let h = Histogram::new("t", "h", ReadingKind::Ratio, FINE);
		for i in 1..=100 {
			h.observe(i as f64 - 0.5); // 0.5, 1.5, ..., 99.5 → lands in buckets [0..100]
		}
		let p = h.percentiles();
		assert_eq!(p.p50, 50.0);
		assert_eq!(p.p99, 99.0);
		assert_eq!(p.max, 100.0);
		assert_eq!(p.p5, 5.0);
		assert_eq!(p.p95, 95.0);
	}

	#[test]
	fn all_in_one_bucket() {
		let h = Histogram::new("t", "h", ReadingKind::Ratio, SIMPLE_BOUNDS);
		for _ in 0..1000 {
			h.observe(15.0); // all land in [10..20] bucket
		}
		let p = h.percentiles();
		assert_eq!(p.p5, 20.0);
		assert_eq!(p.p50, 20.0);
		assert_eq!(p.p99, 20.0);
		assert_eq!(p.max, 20.0);
	}

	#[test]
	fn read_flattens_to_six_uniform_samples() {
		// read() is the sole export: six scalars typed by the declared ReadingKind; full
		// quantile detail stays in-process.
		let h = Histogram::new("profiler.query.duration_us", "h", ReadingKind::Duration, SIMPLE_BOUNDS);
		h.observe(15.0);
		let mut out = Vec::new();
		h.read(&mut out);
		assert_eq!(out.len(), 6);
		let metrics: Vec<&str> = out.iter().map(|s| s.metric).collect();
		assert_eq!(metrics, vec!["count", "sum", "p50", "p95", "p99", "max"]);
		assert!(out.iter().all(|s| s.scope == "profiler.query.duration_us"));
		assert_eq!(out[0].reading.as_f64(), 1.0, "count must be the observation count");
		assert!(
			matches!(out[2].reading, Reading::Duration(_)),
			"a Duration-kind histogram must export its quantiles as typed durations, got {:?}",
			out[2].reading
		);
		assert_eq!(out[2].reading.as_f64(), 20.0, "p50 resolves to the bucket upper bound in microseconds");
		assert_eq!(out[2].reading.unit(), "us");
	}
}
