// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Serialize};
use tdigest::TDigest;

const MAX_CENTROIDS: usize = 100;
const FLUSH_THRESHOLD: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PercentileHistogram {
	digest: TDigest,
	pending: Vec<f64>,
}

impl Default for PercentileHistogram {
	fn default() -> Self {
		Self::new()
	}
}

impl PercentileHistogram {
	pub fn new() -> Self {
		Self {
			digest: TDigest::new_with_size(MAX_CENTROIDS),
			pending: Vec::new(),
		}
	}

	pub fn observe(&mut self, value_us: u32) {
		self.pending.push(value_us as f64);
		if self.pending.len() >= FLUSH_THRESHOLD {
			self.flush();
		}
	}

	pub fn merge(&mut self, other: &Self) {
		let mut combined: Vec<f64> = Vec::with_capacity(self.pending.len() + other.pending.len());
		combined.append(&mut self.pending);
		combined.extend(other.pending.iter().copied());
		let merged = TDigest::merge_digests(vec![self.digest.clone(), other.digest.clone()]);
		self.digest = if combined.is_empty() {
			merged
		} else {
			merged.merge_unsorted(combined)
		};
	}

	pub fn total_count(&self) -> u64 {
		(self.digest.count() as u64).saturating_add(self.pending.len() as u64)
	}

	pub fn is_empty(&self) -> bool {
		self.total_count() == 0
	}

	pub fn percentile(&self, p: f64) -> u32 {
		if self.total_count() == 0 {
			return 0;
		}
		let p = p.clamp(0.0, 1.0);
		let digest_for_read = if self.pending.is_empty() {
			self.digest.clone()
		} else {
			self.digest.clone().merge_unsorted(self.pending.clone())
		};
		let estimate = digest_for_read.estimate_quantile(p);
		estimate.round().max(0.0).min(u32::MAX as f64) as u32
	}

	pub fn percentiles(&self) -> Percentiles {
		if self.total_count() == 0 {
			return Percentiles::default();
		}
		let digest_for_read = if self.pending.is_empty() {
			self.digest.clone()
		} else {
			self.digest.clone().merge_unsorted(self.pending.clone())
		};
		let read = |p: f64| digest_for_read.estimate_quantile(p).round().max(0.0).min(u32::MAX as f64) as u32;
		Percentiles {
			p50: read(0.50),
			p60: read(0.60),
			p70: read(0.70),
			p75: read(0.75),
			p80: read(0.80),
			p85: read(0.85),
			p90: read(0.90),
			p95: read(0.95),
			p98: read(0.98),
			p99: read(0.99),
		}
	}

	pub fn percentiles_duration(&self) -> ProfilerPercentiles {
		let raw = self.percentiles();
		ProfilerPercentiles {
			p50: Duration::from_micros_infallible(raw.p50 as u64),
			p60: Duration::from_micros_infallible(raw.p60 as u64),
			p70: Duration::from_micros_infallible(raw.p70 as u64),
			p75: Duration::from_micros_infallible(raw.p75 as u64),
			p80: Duration::from_micros_infallible(raw.p80 as u64),
			p85: Duration::from_micros_infallible(raw.p85 as u64),
			p90: Duration::from_micros_infallible(raw.p90 as u64),
			p95: Duration::from_micros_infallible(raw.p95 as u64),
			p98: Duration::from_micros_infallible(raw.p98 as u64),
			p99: Duration::from_micros_infallible(raw.p99 as u64),
		}
	}

	fn flush(&mut self) {
		if self.pending.is_empty() {
			return;
		}
		let values = mem::take(&mut self.pending);
		self.digest = self.digest.clone().merge_unsorted(values);
	}
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Percentiles {
	pub p50: u32,
	pub p60: u32,
	pub p70: u32,
	pub p75: u32,
	pub p80: u32,
	pub p85: u32,
	pub p90: u32,
	pub p95: u32,
	pub p98: u32,
	pub p99: u32,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ProfilerPercentiles {
	pub p50: Duration,
	pub p60: Duration,
	pub p70: Duration,
	pub p75: Duration,
	pub p80: Duration,
	pub p85: Duration,
	pub p90: Duration,
	pub p95: Duration,
	pub p98: Duration,
	pub p99: Duration,
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
		assert!(p.p50 <= p.p60);
		assert!(p.p60 <= p.p70);
		assert!(p.p70 <= p.p75);
		assert!(p.p75 <= p.p80);
		assert!(p.p80 <= p.p85);
		assert!(p.p85 <= p.p90);
		assert!(p.p90 <= p.p95);
		assert!(p.p95 <= p.p98);
		assert!(p.p98 <= p.p99);
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
