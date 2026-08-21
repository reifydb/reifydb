// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::catalog::flow::OperatorId, util::bloom::BloomFilter};
use reifydb_value::count::Count;

const EXPECTED_KEYS: usize = 1_000_000;

#[derive(Clone)]
pub struct OperatorKeyFilter(Arc<BloomFilter>);

impl Default for OperatorKeyFilter {
	fn default() -> Self {
		Self::new()
	}
}

impl OperatorKeyFilter {
	pub fn new() -> Self {
		Self(Arc::new(BloomFilter::new(EXPECTED_KEYS)))
	}

	pub fn add(&self, operator: OperatorId, key: &EncodedKey) {
		self.0.add(&(operator.0, key.as_slice()));
	}

	pub fn may_contain(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		self.0.might_contain(&(operator.0, key.as_slice()))
	}

	pub fn fill_ratio(&self) -> f64 {
		self.0.fill_ratio()
	}

	pub fn estimated_keys(&self) -> u64 {
		self.0.estimated_items() as u64
	}

	pub fn metrics(&self) -> OperatorFilterMetrics {
		OperatorFilterMetrics {
			fill_ratio: self.fill_ratio(),
			estimated_keys: Count::new(self.estimated_keys()),
		}
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct OperatorFilterMetrics {
	pub fill_ratio: f64,
	pub estimated_keys: Count,
}
