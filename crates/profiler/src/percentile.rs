// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Serialize};

const SUB_BUCKET_BITS: u32 = 3;
const SUB_BUCKETS: usize = 1 << SUB_BUCKET_BITS;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PercentileHistogram {
	counts: Vec<u64>,
	count: u64,
	max: u32,
}

fn bucket_index(value: u32) -> usize {
	if (value as usize) < SUB_BUCKETS {
		return value as usize;
	}
	let exp = 31 - value.leading_zeros();
	let shift = exp - SUB_BUCKET_BITS;
	let sub = ((value >> shift) as usize) & (SUB_BUCKETS - 1);
	SUB_BUCKETS + (shift as usize) * SUB_BUCKETS + sub
}

fn bucket_midpoint(index: usize) -> u64 {
	if index < SUB_BUCKETS {
		return index as u64;
	}
	let shift = ((index - SUB_BUCKETS) / SUB_BUCKETS) as u32;
	let sub = ((index - SUB_BUCKETS) % SUB_BUCKETS) as u64;
	let low = (SUB_BUCKETS as u64 + sub) << shift;
	low + (1u64 << shift) / 2
}

impl PercentileHistogram {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn observe(&mut self, value_us: u32) {
		let index = bucket_index(value_us);
		if self.counts.len() <= index {
			self.counts.resize(index + 1, 0);
		}
		self.counts[index] += 1;
		self.count += 1;
		self.max = self.max.max(value_us);
	}

	pub fn merge(&mut self, other: &Self) {
		if self.counts.len() < other.counts.len() {
			self.counts.resize(other.counts.len(), 0);
		}
		for (mine, theirs) in self.counts.iter_mut().zip(&other.counts) {
			*mine += theirs;
		}
		self.count += other.count;
		self.max = self.max.max(other.max);
	}

	pub fn total_count(&self) -> u64 {
		self.count
	}

	pub fn is_empty(&self) -> bool {
		self.count == 0
	}

	pub fn percentile(&self, p: f64) -> u32 {
		if self.count == 0 {
			return 0;
		}
		let p = p.clamp(0.0, 1.0);
		if p >= 1.0 {
			return self.max;
		}
		let rank = ((p * self.count as f64).ceil() as u64).max(1);
		let mut seen = 0u64;
		for (index, bucket) in self.counts.iter().enumerate() {
			seen += bucket;
			if seen >= rank {
				return bucket_midpoint(index).min(self.max as u64) as u32;
			}
		}
		self.max
	}

	pub fn percentiles(&self) -> Percentiles {
		Percentiles {
			p50: self.percentile(0.50),
			p75: self.percentile(0.75),
			p90: self.percentile(0.90),
			p95: self.percentile(0.95),
			p98: self.percentile(0.98),
			p99: self.percentile(0.99),
			p100: self.percentile(1.00),
		}
	}

	pub fn percentiles_duration(&self) -> ProfilerPercentiles {
		let raw = self.percentiles();
		ProfilerPercentiles {
			p50: Duration::from_micros_infallible(raw.p50 as u64),
			p75: Duration::from_micros_infallible(raw.p75 as u64),
			p90: Duration::from_micros_infallible(raw.p90 as u64),
			p95: Duration::from_micros_infallible(raw.p95 as u64),
			p98: Duration::from_micros_infallible(raw.p98 as u64),
			p99: Duration::from_micros_infallible(raw.p99 as u64),
			p100: Duration::from_micros_infallible(raw.p100 as u64),
		}
	}
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Percentiles {
	pub p50: u32,
	pub p75: u32,
	pub p90: u32,
	pub p95: u32,
	pub p98: u32,
	pub p99: u32,
	pub p100: u32,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ProfilerPercentiles {
	pub p50: Duration,
	pub p75: Duration,
	pub p90: Duration,
	pub p95: Duration,
	pub p98: Duration,
	pub p99: Duration,
	pub p100: Duration,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty_histogram_returns_zero_for_every_percentile() {
		let h = PercentileHistogram::new();
		assert!(h.is_empty());
		assert_eq!(h.percentile(0.50), 0);
		assert_eq!(h.percentile(0.99), 0);
	}

	#[test]
	fn observe_increments_count() {
		let mut h = PercentileHistogram::new();
		h.observe(10);
		h.observe(20);
		h.observe(30);
		assert_eq!(h.total_count(), 3);
	}

	#[test]
	fn percentile_brackets_observed_range() {
		let mut h = PercentileHistogram::new();
		for v in 1u32..=1000 {
			h.observe(v);
		}
		let p50 = h.percentile(0.50);
		let p99 = h.percentile(0.99);
		assert!((400..=600).contains(&p50), "p50={p50} should bracket the median ~500");
		assert!((900..=1010).contains(&p99), "p99={p99} should bracket ~990");
	}

	#[test]
	fn percentile_does_not_exceed_max_observed() {
		let mut h = PercentileHistogram::new();
		for v in [51u32, 80, 120, 200, 419] {
			h.observe(v);
		}
		let max_observed = 419u32;
		let p99 = h.percentile(0.99);
		assert!(p99 <= max_observed, "p99={p99} exceeded observed max={max_observed}");
	}

	#[test]
	fn merge_combines_counts() {
		let mut a = PercentileHistogram::new();
		a.observe(10);
		a.observe(20);
		let mut b = PercentileHistogram::new();
		b.observe(30);
		b.observe(40);
		a.merge(&b);
		assert_eq!(a.total_count(), 4);
	}

	#[test]
	fn requested_percentiles_are_monotonic() {
		let mut h = PercentileHistogram::new();
		for v in [1u32, 5, 10, 20, 50, 100, 200, 500, 1000, 5000] {
			for _ in 0..50 {
				h.observe(v);
			}
		}
		let p = h.percentiles();
		assert!(p.p50 <= p.p75);
		assert!(p.p75 <= p.p90);
		assert!(p.p90 <= p.p95);
		assert!(p.p95 <= p.p98);
		assert!(p.p98 <= p.p99);
		assert!(p.p99 <= p.p100);
	}

	#[test]
	fn percentile_clamps_p_to_valid_range() {
		let mut h = PercentileHistogram::new();
		h.observe(100);
		assert!(h.percentile(2.0) > 0);
		// negative p clamps to 0.0
		let _ = h.percentile(-1.0);
	}
}
