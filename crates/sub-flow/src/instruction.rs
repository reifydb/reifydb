// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow processing instruction types for targeted worker dispatch.
//!
//! These types enable the coordinator to send only relevant changes to each worker,
//! rather than broadcasting all changes to all workers.

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use reifydb_core::interface::change::Change;

/// Instructions for processing a single flow within a batch.
///
/// Contains all the changes relevant to a specific flow for a version range,
/// along with the version bounds for checkpoint validation.
#[derive(Clone, Debug)]
pub struct FlowInstruction {
	/// The flow to process these changes for
	pub flow_id: FlowId,
	/// Start of version range (exclusive)
	pub to_version: CommitVersion,
	/// The actual changes to process, filtered to only those relevant to this flow.
	/// Changes maintain their original CDC sequence order.
	pub changes: Vec<Change>,
}

impl FlowInstruction {
	/// Create a new flow instruction.
	pub fn new(flow_id: FlowId, to_version: CommitVersion, changes: Vec<Change>) -> Self {
		Self {
			flow_id,
			to_version,
			changes,
		}
	}
}

/// A batch of instructions for a single worker.
///
/// Contains instructions for multiple flows, all of which are assigned to the same worker
/// via hash partitioning (flow_id % num_workers).
#[derive(Clone, Debug)]
pub struct WorkerBatch {
	/// The version to use for reading flow state.
	/// This is constant for the entire CDC batch being processed.
	pub state_version: CommitVersion,
	/// Instructions for each flow assigned to this worker.
	/// Each flow appears at most once in this list.
	pub instructions: Vec<FlowInstruction>,
}

impl WorkerBatch {
	/// Create a new empty worker batch.
	pub fn new(state_version: CommitVersion) -> Self {
		Self {
			state_version,
			instructions: Vec::new(),
		}
	}

	/// Add an instruction to this batch.
	pub fn add_instruction(&mut self, instruction: FlowInstruction) {
		self.instructions.push(instruction);
	}
}
