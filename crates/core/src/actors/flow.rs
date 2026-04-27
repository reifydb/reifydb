// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Actor messages and types for the flow processing subsystem.

use std::collections::BTreeMap;

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_type::{Result, value::datetime::DateTime};

use super::pending::Pending;
use crate::{
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{catalog::flow::FlowId, cdc::Cdc, change::Change},
};

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

/// Response from a flow worker actor.
pub enum FlowResponse {
	/// Operation succeeded with pending writes, pending shapes, and view changes
	Success {
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
	},
	/// Operation failed with error message
	Error(String),
}

/// Handle to the flow worker actor.
pub type FlowHandle = ActorHandle<FlowMessage>;

/// Messages for the flow worker actor.
pub enum FlowMessage {
	/// Process a batch of flow instructions
	Process {
		batch: WorkerBatch,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},
	/// Register a new flow by ID (worker looks up the FlowDag from the registry)
	Register {
		flow_id: FlowId,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},
	/// Process periodic tick for time-based maintenance
	Tick {
		flow_ids: Vec<FlowId>,
		timestamp: DateTime,
		state_version: CommitVersion,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},
	Rebalance {
		flow_ids: Vec<FlowId>,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},
}

/// Response from the flow pool actor.
pub enum PoolResponse {
	/// Operation succeeded with pending writes, pending shapes, and view changes
	Success {
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
	},
	/// Registration succeeded
	RegisterSuccess,
	/// Operation failed with error message
	Error(String),
}

/// Handle to the flow pool actor.
pub type FlowPoolHandle = ActorHandle<FlowPoolMessage>;

/// Messages for the flow pool actor.
pub enum FlowPoolMessage {
	/// Register a new flow by ID (routes to appropriate worker)
	RegisterFlow {
		flow_id: FlowId,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},
	/// Submit batches to multiple workers
	Submit {
		batches: BTreeMap<usize, WorkerBatch>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},
	/// Submit to a specific worker
	SubmitToWorker {
		worker_id: usize,
		batch: WorkerBatch,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},
	/// Process periodic tick for time-based maintenance
	Tick {
		ticks: BTreeMap<usize, Vec<FlowId>>,
		timestamp: DateTime,
		state_version: CommitVersion,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},
	Rebalance {
		assignments: BTreeMap<usize, Vec<FlowId>>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},
	/// Async reply from a FlowActor worker
	WorkerReply {
		worker_id: usize,
		response: FlowResponse,
	},
}

/// Handle to the flow coordinator actor.
pub type FlowCoordinatorHandle = ActorHandle<FlowCoordinatorMessage>;

/// Messages for the flow coordinator actor.
pub enum FlowCoordinatorMessage {
	/// Consume CDC events and process them through flows
	Consume {
		cdcs: Vec<Cdc>,
		current_version: CommitVersion,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	},
	/// Async reply from PoolActor
	PoolReply(PoolResponse),
	/// Periodic tick for time-based maintenance
	Tick,
}
