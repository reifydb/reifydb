// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::{GroupStateKey, KeyspaceId},
};
use reifydb_value::byte_size::ByteSize;

#[derive(Debug, Clone)]
pub struct OperatorBatch {
	pub items: Vec<(GroupStateKey, EncodedPodRow)>,
	pub has_more: bool,
	pub resume: Option<GroupStateKey>,
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
pub struct BufferedRange {
	pub items: Vec<(GroupStateKey, Option<EncodedPodRow>)>,
	pub dropped: bool,
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
pub enum LayeredPre {
	Absent,
	Present(ByteSize),
}

#[derive(Debug, Clone)]
pub enum OperatorWrite {
	Insert {
		operator: OperatorId,
		key: GroupStateKey,
		post: EncodedPodRow,
	},
	Replace {
		operator: OperatorId,
		key: GroupStateKey,
		pre_value_bytes: ByteSize,
		post: EncodedPodRow,
	},
	Remove {
		operator: OperatorId,
		key: GroupStateKey,
		pre: LayeredPre,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Applied {
	pub rows: usize,
	pub bytes: ByteSize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagedWrite {
	Set(EncodedPodRow),
	Remove,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
	pub rows: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resume {
	Done,
	More,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scan {
	Forward,
	Backward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropMarker {
	OperatorState(OperatorId),
}

#[derive(Default)]
pub struct FlushBatch {
	pub writes: Vec<(OperatorId, GroupStateKey, StagedWrite)>,
	pub checkpoints: BTreeMap<FlowId, Option<CommitVersion>>,
	pub drops: Vec<DropMarker>,
	pub bytes: ByteSize,
}

impl FlushBatch {
	pub fn is_empty(&self) -> bool {
		self.writes.is_empty() && self.checkpoints.is_empty() && self.drops.is_empty()
	}
}
