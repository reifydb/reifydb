// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::GroupId,
};
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use crate::{
	tier::bucket::BucketMap,
	types::{JOIN_EXPIRY_KEY_BYTES, JOIN_EXPIRY_VALUE_BYTES},
};

pub type JoinExpiryKey = (OperatorId, GroupId, u8, RowNumber);

pub type JoinExpirySlot = (u8, RowNumber);

pub const JOIN_EXPIRY_ENTRY_BYTES: ByteSize = JOIN_EXPIRY_KEY_BYTES.saturating_add(JOIN_EXPIRY_VALUE_BYTES);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropMarker {
	OperatorState(OperatorId),
	JoinExpiriesOperator(OperatorId),
	JoinExpiriesGroup(OperatorId, GroupId),
}

#[derive(Default)]
pub struct FlushBatch {
	pub state: BucketMap,
	pub join_expiries: BTreeMap<JoinExpiryKey, Option<u64>>,
	pub checkpoints: BTreeMap<FlowId, Option<CommitVersion>>,
	pub drops: Vec<DropMarker>,
	pub bytes: ByteSize,
}

impl FlushBatch {
	pub fn is_empty(&self) -> bool {
		self.state.is_empty()
			&& self.join_expiries.is_empty()
			&& self.checkpoints.is_empty()
			&& self.drops.is_empty()
	}
}


