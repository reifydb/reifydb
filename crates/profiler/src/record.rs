// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::profiler::ProfilerCategoryId;
use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Serialize};

use crate::{
	category::ProfilerCategory,
	percentile::{PercentileHistogram, ProfilerPercentiles},
};

pub type DimIdx = u32;
pub const DIM_UNSET: DimIdx = 0;
pub const MAX_DIMENSIONS: usize = 2;
pub const MAX_EXTRAS: usize = 4;

#[repr(C)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MinimalSpanRecord {
	pub category_id: u8,
	pub callsite_id: u64,
	pub duration_us: u32,
	pub dim_indices: [DimIdx; MAX_DIMENSIONS],
	pub extras: [u64; MAX_EXTRAS],
}

impl MinimalSpanRecord {
	pub const fn new(category: ProfilerCategory, callsite_id: u64, duration_us: u32) -> Self {
		Self {
			category_id: category as u8,
			callsite_id,
			duration_us,
			dim_indices: [DIM_UNSET; MAX_DIMENSIONS],
			extras: [0; MAX_EXTRAS],
		}
	}

	pub fn with_dimensions(mut self, dim_indices: [DimIdx; MAX_DIMENSIONS]) -> Self {
		self.dim_indices = dim_indices;
		self
	}

	pub fn with_extras(mut self, extras: [u64; MAX_EXTRAS]) -> Self {
		self.extras = extras;
		self
	}

	pub fn category(&self) -> ProfilerCategory {
		ProfilerCategory::from_id(ProfilerCategoryId(self.category_id))
			.expect("MinimalSpanRecord must hold a valid ProfilerCategory id")
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct SpanIdent {
	pub category: ProfilerCategory,
	pub callsite_id: u64,
	pub dim_indices: [DimIdx; MAX_DIMENSIONS],
}

impl SpanIdent {
	pub const fn new(category: ProfilerCategory, callsite_id: u64, dim_indices: [DimIdx; MAX_DIMENSIONS]) -> Self {
		Self {
			category,
			callsite_id,
			dim_indices,
		}
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateRecord {
	pub category: ProfilerCategory,
	pub span_name: String,
	pub dimensions: Vec<String>,
	pub calls: u64,
	pub total_us: u64,
	pub histogram: PercentileHistogram,
	pub extras_sum: [u64; MAX_EXTRAS],
}

impl AggregateRecord {
	pub fn fold(&mut self, duration_us: u32, extras: &[u64; MAX_EXTRAS]) {
		self.calls = self.calls.saturating_add(1);
		self.total_us = self.total_us.saturating_add(duration_us as u64);
		self.histogram.observe(duration_us);
		for (sum, &extra) in self.extras_sum.iter_mut().zip(extras.iter()) {
			*sum = sum.saturating_add(extra);
		}
	}

	pub fn total(&self) -> Duration {
		Duration::from_micros_infallible(self.total_us)
	}

	pub fn min(&self) -> Duration {
		Duration::from_micros_infallible(self.histogram.percentile(0.0) as u64)
	}

	pub fn max(&self) -> Duration {
		Duration::from_micros_infallible(self.histogram.percentile(1.0) as u64)
	}

	pub fn percentiles(&self) -> ProfilerPercentiles {
		self.histogram.percentiles_duration()
	}

	pub fn extras(&self) -> &[u64; MAX_EXTRAS] {
		&self.extras_sum
	}
}

#[cfg(test)]
mod tests {
	use std::mem::size_of;

	use super::*;
	use crate::category::ALL_CATEGORIES;

	#[test]
	fn minimal_span_record_size_is_64_bytes() {
		assert_eq!(size_of::<MinimalSpanRecord>(), 64);
	}

	#[test]
	fn aggregate_fold_tracks_calls_and_distribution() {
		let mut agg = AggregateRecord {
			category: ProfilerCategory::Flow,
			span_name: "flow::engine::apply".to_string(),
			dimensions: vec!["map".to_string(), "n1".to_string()],
			calls: 0,
			total_us: 0,
			histogram: PercentileHistogram::new(),
			extras_sum: [0; MAX_EXTRAS],
		};
		agg.fold(100, &[10, 20, 0, 0]);
		agg.fold(50, &[5, 10, 0, 0]);
		agg.fold(200, &[2, 4, 0, 0]);

		assert_eq!(agg.calls, 3);
		assert_eq!(agg.total_us, 350);
		assert_eq!(agg.extras_sum, [17, 34, 0, 0]);
		assert_eq!(agg.histogram.total_count(), 3);
		let p = agg.histogram.percentiles();
		assert!(p.p50 <= p.p90, "p50 should not exceed p90");
		assert!(p.p90 <= p.p99, "p90 should not exceed p99");
	}

	#[test]
	fn category_round_trip_through_record() {
		for cat in ALL_CATEGORIES {
			let rec = MinimalSpanRecord::new(cat, 42, 99);
			assert_eq!(rec.category(), cat);
		}
	}
}
