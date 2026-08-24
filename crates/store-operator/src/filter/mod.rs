// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod source;

use std::sync::Arc;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId, key::operator_state::GroupId, util::bloom::hash_item,
};
use reifydb_filter::adaptive::{AdaptiveKeyFilter, FilterMetrics};
use reifydb_value::value::row_number::RowNumber;

pub const ARMED_CAPACITY_KEYS: u64 = 1_000_000;

pub const ARMED_CAPACITY_ANCHORS: u64 = 1_000_000;

pub(crate) fn hash_key(operator: OperatorId, key: &EncodedKey) -> u64 {
	hash_item(&(operator.0, key.as_slice()))
}

pub(crate) fn hash_anchor(operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) -> u64 {
	hash_item(&(operator.0, group.0, side, row_number.0))
}

#[derive(Clone)]
pub struct OperatorKeyFilter(Arc<AdaptiveKeyFilter>);

impl OperatorKeyFilter {
	#[allow(clippy::new_without_default)]
	pub fn new() -> Self {
		Self(Arc::new(AdaptiveKeyFilter::new()))
	}

	pub fn armed(size_for_keys: u64) -> Self {
		Self(Arc::new(AdaptiveKeyFilter::armed(size_for_keys)))
	}

	pub fn add(&self, operator: OperatorId, key: &EncodedKey) {
		self.0.add(hash_key(operator, key));
	}

	pub fn may_contain(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		self.0.may_contain(hash_key(operator, key))
	}

	pub fn metrics(&self) -> FilterMetrics {
		self.0.metrics()
	}

	pub fn handle(&self) -> Arc<AdaptiveKeyFilter> {
		self.0.clone()
	}
}

#[derive(Clone)]
pub struct OperatorAnchorFilter(Arc<AdaptiveKeyFilter>);

impl OperatorAnchorFilter {
	#[allow(clippy::new_without_default)]
	pub fn new() -> Self {
		Self(Arc::new(AdaptiveKeyFilter::new()))
	}

	pub fn armed(size_for_keys: u64) -> Self {
		Self(Arc::new(AdaptiveKeyFilter::armed(size_for_keys)))
	}

	pub fn add(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		self.0.add(hash_anchor(operator, group, side, row_number));
	}

	pub fn may_contain(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) -> bool {
		self.0.may_contain(hash_anchor(operator, group, side, row_number))
	}

	pub fn metrics(&self) -> FilterMetrics {
		self.0.metrics()
	}

	pub fn handle(&self) -> Arc<AdaptiveKeyFilter> {
		self.0.clone()
	}
}
