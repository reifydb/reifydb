// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_wal::lsn::Lsn;

use crate::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::GroupStateKey,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowCheckpoint {
	pub version: CommitVersion,
	pub lsn: Lsn,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperatorSection {
	pub slices: Vec<OperatorSlice>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSlice {
	pub writes: Vec<(OperatorId, GroupStateKey, OperatorStateWrite)>,
	pub checkpoints: Vec<(FlowId, FlowCheckpoint)>,
	pub deletes: Vec<FlowId>,
}

impl OperatorSlice {
	pub fn is_empty(&self) -> bool {
		self.writes.is_empty() && self.checkpoints.is_empty() && self.deletes.is_empty()
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorStateWrite {
	Set(EncodedPodRow),
	Remove,
}
