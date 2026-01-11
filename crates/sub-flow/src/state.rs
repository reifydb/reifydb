// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow state tracking for the coordinator.
//!
//! Tracks the status and checkpoint of each flow, enabling the coordinator
//! to route changes appropriately and manage backfilling.

use reifydb_core::{CommitVersion, interface::FlowId};
use std::collections::HashMap;

/// Status of a flow in the coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlowStatus {
	/// Flow is active and receiving live CDC events.
	Active,
	/// Flow is catching up from historical CDC data.
	/// It will not receive live CDC events until it catches up to the current version.
	Backfilling,
}

/// State of a single flow tracked by the coordinator.
#[derive(Clone, Debug)]
pub struct FlowState {
	/// Current checkpoint version - the last version this flow has processed.
	pub checkpoint: CommitVersion,
	/// Current status of the flow.
	pub status: FlowStatus,
}

impl FlowState {
	/// Create a new flow state for a newly registered flow.
	///
	/// New flows start in Backfilling status with checkpoint 0.
	pub fn new_backfilling() -> Self {
		Self {
			checkpoint: CommitVersion(0),
			status: FlowStatus::Backfilling,
		}
	}

	/// Check if the flow is currently backfilling.
	pub fn is_backfilling(&self) -> bool {
		self.status == FlowStatus::Backfilling
	}

	/// Check if the flow is active (receiving live CDC).
	pub fn is_active(&self) -> bool {
		self.status == FlowStatus::Active
	}

	/// Transition the flow to active status.
	pub fn activate(&mut self) {
		self.status = FlowStatus::Active;
	}

	/// Update the checkpoint to a new version.
	pub fn update_checkpoint(&mut self, version: CommitVersion) {
		self.checkpoint = version;
	}
}

/// Collection of flow states managed by the coordinator.
#[derive(Debug, Default)]
pub struct FlowStates {
	states: HashMap<FlowId, FlowState>,
}

impl FlowStates {
	/// Create a new empty flow states collection.
	pub fn new() -> Self {
		Self {
			states: HashMap::new(),
		}
	}

	/// Get mutable access to the state of a flow.
	pub fn get_mut(&mut self, flow_id: &FlowId) -> Option<&mut FlowState> {
		self.states.get_mut(flow_id)
	}

	/// Register a new flow in backfilling status.
	pub fn register_backfilling(&mut self, flow_id: FlowId) {
		self.states.insert(flow_id, FlowState::new_backfilling());
	}

	/// Get all active flow IDs.
	pub fn active_flow_ids(&self) -> Vec<FlowId> {
		self.states.iter().filter(|(_, state)| state.is_active()).map(|(id, _)| *id).collect()
	}

	/// Get all backfilling flow IDs.
	pub fn backfilling_flow_ids(&self) -> Vec<FlowId> {
		self.states.iter().filter(|(_, state)| state.is_backfilling()).map(|(id, _)| *id).collect()
	}
}
