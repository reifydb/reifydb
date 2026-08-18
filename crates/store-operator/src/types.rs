// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace},
};
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::commit::batch::AnchorSlot;

pub const ANCHOR_KEY_BYTES: ByteSize = ByteSize::from_bytes(25);

pub const ANCHOR_VALUE_BYTES: ByteSize = ByteSize::from_bytes(8);

#[derive(Debug, Clone)]
pub struct OperatorBatch {
	pub items: Vec<(EncodedKey, EncodedPodRow)>,
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

#[derive(Debug, Clone, PartialEq)]
pub enum BufferedState {
	Row(EncodedPodRow),
	Tombstone,
	Dropped,
	Absent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BufferedStateRange {
	pub items: Vec<(EncodedKey, Option<EncodedPodRow>)>,
	pub dropped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BufferedAnchor {
	Expiry(u64),
	Tombstone,
	Dropped,
	Absent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedAnchorGroup {
	pub anchors: Vec<(AnchorSlot, Option<u64>)>,
	pub dropped: bool,
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
	pub keys: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateCensus {
	pub operator: OperatorId,
	pub keyspace: Keyspace,
	pub keys: u64,
	pub key_bytes: ByteSize,
	pub value_bytes: ByteSize,
}

#[derive(Debug, Clone)]
pub enum OperatorWrite {
	Set {
		operator: OperatorId,
		key: EncodedKey,
		row: EncodedPodRow,
	},
	Remove {
		operator: OperatorId,
		key: EncodedKey,
	},
	AnchorSet {
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_num: RowNumber,
		expiry: DateTime,
	},
	AnchorRemove {
		operator: OperatorId,
		group: GroupId,
		side: u8,
		run_num: RowNumber,
	},
}
