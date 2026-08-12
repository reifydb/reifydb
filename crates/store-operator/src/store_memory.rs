// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_runtime::shutdown::Shutdown;

#[derive(Debug, Clone)]
pub struct OperatorBatch {
	pub items: Vec<(EncodedKey, EncodedOperatorRow)>,
	pub has_more: bool,
}

impl OperatorBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateCensus {
	pub operator: OperatorId,
	pub prefix: Vec<u8>,
	pub keys: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
}

#[derive(Clone)]
pub struct OperatorStore;

impl Default for OperatorStore {
	fn default() -> Self {
		Self::memory()
	}
}

impl OperatorStore {
	pub fn memory() -> Self {
		Self
	}

	pub fn set(&self, _operator: OperatorId, _key: EncodedKey, _row: EncodedOperatorRow) {}

	pub fn remove(&self, _operator: OperatorId, _key: &EncodedKey) {}

	pub fn get(&self, _operator: OperatorId, _key: &EncodedKey) -> Option<EncodedOperatorRow> {
		None
	}

	pub fn contains(&self, _operator: OperatorId, _key: &EncodedKey) -> bool {
		false
	}

	pub fn range_batch(&self, _operator: OperatorId, _range: EncodedKeyRange, _batch_size: u64) -> OperatorBatch {
		OperatorBatch::empty()
	}

	pub fn bytes(&self, _operator: OperatorId) -> u64 {
		0
	}

	pub fn total_bytes(&self) -> u64 {
		0
	}

	pub fn census(&self, _prefix_len: u32) -> Vec<OperatorStateCensus> {
		Vec::new()
	}

	pub fn drop_operator_state(&self, _operator: OperatorId) {}
}

impl Shutdown for OperatorStore {
	fn shutdown(&self) {}
}
