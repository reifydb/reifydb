// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlowStatus {
	Active,

	Backfilling,
}

#[derive(Clone, Debug)]
pub struct FlowState {
	pub checkpoint: CommitVersion,

	pub status: FlowStatus,
}

impl FlowState {
	pub fn new_backfilling() -> Self {
		Self {
			checkpoint: CommitVersion(0),
			status: FlowStatus::Backfilling,
		}
	}

	pub fn new_active(checkpoint: CommitVersion) -> Self {
		Self {
			checkpoint,
			status: FlowStatus::Active,
		}
	}

	pub fn is_backfilling(&self) -> bool {
		self.status == FlowStatus::Backfilling
	}

	pub fn is_active(&self) -> bool {
		self.status == FlowStatus::Active
	}

	pub fn activate(&mut self) {
		self.status = FlowStatus::Active;
	}

	pub fn update_checkpoint(&mut self, version: CommitVersion) {
		self.checkpoint = version;
	}
}

#[derive(Debug, Default)]
pub struct FlowStates {
	states: BTreeMap<FlowId, FlowState>,
}

impl FlowStates {
	pub fn new() -> Self {
		Self {
			states: BTreeMap::new(),
		}
	}

	pub fn get_mut(&mut self, flow_id: &FlowId) -> Option<&mut FlowState> {
		self.states.get_mut(flow_id)
	}

	pub fn contains(&self, flow_id: &FlowId) -> bool {
		self.states.contains_key(flow_id)
	}

	pub fn register_backfilling(&mut self, flow_id: FlowId) {
		self.states.insert(flow_id, FlowState::new_backfilling());
	}

	pub fn register_active(&mut self, flow_id: FlowId, checkpoint: CommitVersion) {
		self.states.insert(flow_id, FlowState::new_active(checkpoint));
	}

	pub fn active_flow_ids(&self) -> Vec<FlowId> {
		self.states.iter().filter(|(_, state)| state.is_active()).map(|(id, _)| *id).collect()
	}

	pub fn backfilling_flow_ids(&self) -> Vec<FlowId> {
		self.states.iter().filter(|(_, state)| state.is_backfilling()).map(|(id, _)| *id).collect()
	}
}
