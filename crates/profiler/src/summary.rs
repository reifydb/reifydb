// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Serialize};

use crate::{
	category::ProfilerCategory,
	intern::DimInterner,
	record::{MAX_EXTRAS, MinimalSpanRecord},
	scope::ScopeId,
};

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
pub struct CategorySummary {
	pub calls: u64,
	pub total_us: u64,
	pub min_us: u32,
	pub max_us: u32,
	pub extras_sum: [u64; MAX_EXTRAS],
}

impl CategorySummary {
	pub fn fold(&mut self, duration_us: u32, extras: &[u64; MAX_EXTRAS]) {
		let was_empty = self.calls == 0;
		self.calls = self.calls.saturating_add(1);
		self.total_us = self.total_us.saturating_add(duration_us as u64);
		if was_empty || duration_us < self.min_us {
			self.min_us = duration_us;
		}
		if duration_us > self.max_us {
			self.max_us = duration_us;
		}
		for (sum, &extra) in self.extras_sum.iter_mut().zip(extras.iter()) {
			*sum = sum.saturating_add(extra);
		}
	}

	pub fn total(&self) -> Duration {
		Duration::from_micros_infallible(self.total_us)
	}

	pub fn min(&self) -> Duration {
		Duration::from_micros_infallible(self.min_us as u64)
	}

	pub fn max(&self) -> Duration {
		Duration::from_micros_infallible(self.max_us as u64)
	}

	pub fn extras(&self) -> &[u64; MAX_EXTRAS] {
		&self.extras_sum
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerSummary {
	pub scope_id: ScopeId,
	pub scope_name: &'static str,
	pub started_at_nanos: u128,
	pub total_duration_us: u64,
	pub records: Vec<MinimalSpanRecord>,
	pub per_category: [CategorySummary; 6],
	#[serde(skip)]
	pub interner: Option<Arc<DimInterner>>,
}

impl ProfilerSummary {
	pub fn category(&self, c: ProfilerCategory) -> CategorySummary {
		self.per_category[c as usize]
	}

	pub fn total_calls(&self) -> u64 {
		self.per_category.iter().map(|c| c.calls).sum()
	}

	pub fn from_records(
		scope_id: ScopeId,
		scope_name: &'static str,
		started_at_nanos: u128,
		total_duration_us: u64,
		records: Vec<MinimalSpanRecord>,
		interner: Option<Arc<DimInterner>>,
	) -> Self {
		let mut per_category = [CategorySummary::default(); 6];
		for rec in &records {
			let idx = rec.category_id as usize;
			if idx < per_category.len() {
				per_category[idx].fold(rec.duration_us, &rec.extras);
			}
		}
		Self {
			scope_id,
			scope_name,
			started_at_nanos,
			total_duration_us,
			records,
			per_category,
			interner,
		}
	}

	pub fn flow_category() -> ProfilerCategory {
		ProfilerCategory::Flow
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::category::ALL_CATEGORIES;

	#[test]
	fn category_summary_fold_tracks_extremes() {
		let mut s = CategorySummary::default();
		s.fold(100, &[1, 2, 3, 4]);
		s.fold(50, &[1, 1, 1, 1]);
		s.fold(200, &[0, 0, 0, 0]);
		assert_eq!(s.calls, 3);
		assert_eq!(s.total_us, 350);
		assert_eq!(s.min_us, 50);
		assert_eq!(s.max_us, 200);
		assert_eq!(s.extras_sum, [2, 3, 4, 5]);
	}

	#[test]
	fn summary_from_records_aggregates_per_category() {
		let records = vec![
			MinimalSpanRecord::new(ProfilerCategory::Flow, 1, 100).with_extras([10, 20, 0, 0]),
			MinimalSpanRecord::new(ProfilerCategory::Flow, 2, 50).with_extras([5, 10, 0, 0]),
			MinimalSpanRecord::new(ProfilerCategory::Query, 3, 30),
		];
		let summary = ProfilerSummary::from_records(ScopeId(7), "test", 0, 1000, records, None);
		assert_eq!(summary.category(ProfilerCategory::Flow).calls, 2);
		assert_eq!(summary.category(ProfilerCategory::Flow).total_us, 150);
		assert_eq!(summary.category(ProfilerCategory::Query).calls, 1);
		assert_eq!(summary.category(ProfilerCategory::Storage).calls, 0);
		assert_eq!(summary.total_calls(), 3);
	}

	#[test]
	fn all_categories_addressable() {
		let mut per = [CategorySummary::default(); 6];
		for c in ALL_CATEGORIES {
			per[c as usize].calls = c as u64;
		}
		for c in ALL_CATEGORIES {
			assert_eq!(per[c as usize].calls, c as u64);
		}
	}
}
