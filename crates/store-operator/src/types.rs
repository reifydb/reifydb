// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::{
		catalog::flow::{FlowId, OperatorId},
		operator::{FlowCheckpoint, OperatorSlice, OperatorStateWrite},
	},
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId},
};
use reifydb_value::byte_size::ByteSize;
use reifydb_wal::lsn::Lsn;

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

pub fn classify_slice(slice: &OperatorSlice, sizes: &[Option<ByteSize>]) -> Vec<OperatorWrite> {
	assert_eq!(
		slice.writes.len(),
		sizes.len(),
		"operator slice classification got {} pre sizes for {} writes",
		sizes.len(),
		slice.writes.len()
	);
	slice.writes
		.iter()
		.zip(sizes)
		.map(|((operator, key, write), size)| match (write, size) {
			(OperatorStateWrite::Set(post), Some(pre_value_bytes)) => OperatorWrite::Replace {
				operator: *operator,
				key: key.clone(),
				pre_value_bytes: *pre_value_bytes,
				post: post.clone(),
			},
			(OperatorStateWrite::Set(post), None) => OperatorWrite::Insert {
				operator: *operator,
				key: key.clone(),
				post: post.clone(),
			},
			(OperatorStateWrite::Remove, Some(bytes)) => OperatorWrite::Remove {
				operator: *operator,
				key: key.clone(),
				pre: LayeredPre::Present(*bytes),
			},
			(OperatorStateWrite::Remove, None) => OperatorWrite::Remove {
				operator: *operator,
				key: key.clone(),
				pre: LayeredPre::Absent,
			},
		})
		.collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredRows {
	pub operator: OperatorId,
	pub keyspace: KeyspaceId,
	pub keys: u64,
	pub value_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Applied {
	pub rows: usize,
	pub bytes: ByteSize,
	pub retired: Vec<RetiredRows>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagedWrite {
	Set(EncodedPodRow),
	Remove,
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
	pub checkpoints: BTreeMap<FlowId, Option<FlowCheckpoint>>,
	pub drops: Vec<DropMarker>,
	pub retired: Vec<(OperatorId, GroupId)>,
	pub bytes: ByteSize,
	pub through: Option<Lsn>,
}

impl FlushBatch {
	pub fn is_empty(&self) -> bool {
		self.writes.is_empty()
			&& self.checkpoints.is_empty()
			&& self.drops.is_empty()
			&& self.retired.is_empty()
	}
}
