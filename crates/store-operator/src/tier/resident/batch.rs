// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::GroupId,
};
use reifydb_value::{byte_size::ByteSize, value::row_number::RowNumber};

use crate::{
	tier::resident::state_map::StateMap,
	types::{DurablePre, JOIN_EXPIRY_KEY_BYTES, JOIN_EXPIRY_VALUE_BYTES},
};

pub type StateKey = (OperatorId, EncodedKey);

pub type JoinExpiryKey = (OperatorId, GroupId, u8, RowNumber);

pub type JoinExpirySlot = (u8, RowNumber);

pub const JOIN_EXPIRY_ENTRY_BYTES: ByteSize = JOIN_EXPIRY_KEY_BYTES.saturating_add(JOIN_EXPIRY_VALUE_BYTES);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropMarker {
	OperatorState(OperatorId),
	JoinExpiriesOperator(OperatorId),
	JoinExpiriesGroup(OperatorId, GroupId),
}

#[derive(Debug, Clone)]
pub struct StateEntry {
	pub post: Option<EncodedPodRow>,
	pub durable_pre: DurablePre,
}

#[derive(Debug, Default)]
pub struct FlushBatch {
	pub state: StateMap,
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

fn post_bytes(post: &Option<EncodedPodRow>) -> ByteSize {
	post.as_ref().map_or(ByteSize::ZERO, |row| ByteSize::from_bytes(row.bytes().len() as u64))
}

pub(crate) fn state_entry_bytes(key: &EncodedKey, entry: &StateEntry) -> ByteSize {
	ByteSize::from_bytes(key.len() as u64).saturating_add(post_bytes(&entry.post))
}
