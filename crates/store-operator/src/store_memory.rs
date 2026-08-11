// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{common::CommitVersion, interface::catalog::flow::OperatorId};
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

#[derive(Clone)]
pub struct OperatorStore;

impl Default for OperatorStore {
	fn default() -> Self {
		Self::testing_memory()
	}
}

impl OperatorStore {
	pub fn memory() -> Self {
		Self
	}

	pub fn testing_memory() -> Self {
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

	pub fn drop_arena(&self, _operator: OperatorId) {}
}

impl Shutdown for OperatorStore {
	fn shutdown(&self) {}
}
