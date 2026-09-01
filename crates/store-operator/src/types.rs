// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, KeyspaceId},
};
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::tier::resident::batch::JoinExpirySlot;

pub const JOIN_EXPIRY_KEY_BYTES: ByteSize = ByteSize::from_bytes(33);

pub const JOIN_EXPIRY_VALUE_BYTES: ByteSize = ByteSize::from_bytes(8);

#[derive(Debug, Clone)]
pub struct OperatorBatch {
	pub items: Vec<(EncodedKey, EncodedPodRow)>,
	pub has_more: bool,
	pub resume: Option<EncodedKey>,
}

impl OperatorBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
			resume: None,
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
pub enum BufferedJoinExpiry {
	Expiry(u64),
	Tombstone,
	Dropped,
	Absent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedJoinExpiryGroup {
	pub join_expiries: Vec<(JoinExpirySlot, Option<u64>)>,
	pub dropped: bool,
	pub durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredJoinRowExpiry {
	pub side: u8,
	pub row_number: RowNumber,
	pub at: DateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredJoinRowExpiryCensus {
	pub operator: OperatorId,
	pub keys: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateCensus {
	pub operator: OperatorId,
	pub keyspace: KeyspaceId,
	pub keys: u64,
	pub key_bytes: ByteSize,
	pub value_bytes: ByteSize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurablePre {
	Absent,
	Present(ByteSize),
}

#[derive(Debug, Clone)]
pub enum OperatorWrite {
	Insert {
		operator: OperatorId,
		key: EncodedKey,
		post: EncodedPodRow,
	},
	Replace {
		operator: OperatorId,
		key: EncodedKey,
		pre_value_bytes: ByteSize,
		post: EncodedPodRow,
	},
	Remove {
		operator: OperatorId,
		key: EncodedKey,
		pre: DurablePre,
	},
	JoinExpiryInsert {
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_num: RowNumber,
		at: DateTime,
	},
	JoinExpiryReplace {
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_num: RowNumber,
		at: DateTime,
	},
	JoinExpiryRemove {
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_num: RowNumber,
		pre: DurablePre,
	},
}
