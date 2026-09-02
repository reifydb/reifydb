// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
};
use reifydb_value::byte_size::ByteSize;

use crate::tier::bucket::BucketMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropMarker {
	OperatorState(OperatorId),
}

#[derive(Default)]
pub struct FlushBatch {
	pub state: BucketMap,
	pub checkpoints: BTreeMap<FlowId, Option<CommitVersion>>,
	pub drops: Vec<DropMarker>,
	pub bytes: ByteSize,
}

impl FlushBatch {
	pub fn is_empty(&self) -> bool {
		self.state.is_empty() && self.checkpoints.is_empty() && self.drops.is_empty()
	}
}
