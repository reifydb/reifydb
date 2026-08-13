// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

pub const ANCHOR_KEY_BYTES: u64 = 25;

pub const ANCHOR_VALUE_BYTES: u64 = 8;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateCensus {
	pub operator: OperatorId,
	pub prefix: Vec<u8>,
	pub keys: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
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
