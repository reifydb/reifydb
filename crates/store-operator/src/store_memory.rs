// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_runtime::shutdown::Shutdown;
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

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

pub const ANCHOR_KEY_BYTES: u64 = 25;

pub const ANCHOR_VALUE_BYTES: u64 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSealAnchor {
	pub side: u8,
	pub row_number: RowNumber,
	pub expiry: DateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSealAnchorCensus {
	pub operator: OperatorId,
	pub group: GroupId,
	pub keys: u64,
}

#[derive(Debug, Clone)]
pub enum OperatorWrite {
	Set {
		operator: OperatorId,
		key: EncodedKey,
		row: EncodedOperatorRow,
	},
	Remove {
		operator: OperatorId,
		key: EncodedKey,
	},
	AnchorSet {
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	},
	AnchorRemove {
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	},
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

	pub fn apply_batch(&self, _writes: &[OperatorWrite]) {}

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

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		Vec::new()
	}

	pub fn anchor_get(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_side: u8,
		_row_number: RowNumber,
	) -> Option<DateTime> {
		None
	}

	pub fn anchors_by_expiry(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_limit: u64,
	) -> Vec<OperatorSealAnchor> {
		Vec::new()
	}

	pub fn anchors_due(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_at: DateTime,
		_limit: u64,
	) -> Vec<OperatorSealAnchor> {
		Vec::new()
	}

	pub fn anchor_set(
		&self,
		_operator: OperatorId,
		_group: GroupId,
		_side: u8,
		_row_number: RowNumber,
		_expiry: DateTime,
	) {
	}

	pub fn anchor_remove(&self, _operator: OperatorId, _group: GroupId, _side: u8, _row_number: RowNumber) {}

	pub fn anchors_remove_group(&self, _operator: OperatorId, _group: GroupId) {}

	pub fn anchors_drop_operator(&self, _operator: OperatorId) {}

	pub fn drop_operator_state(&self, _operator: OperatorId) {}
}

impl Shutdown for OperatorStore {
	fn shutdown(&self) {}
}
