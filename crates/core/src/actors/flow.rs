// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::BTreeMap;

use reifydb_runtime::actor::system::ActorHandle;
use reifydb_type::{Result, value::datetime::DateTime};

use super::pending::Pending;
use crate::{
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{catalog::flow::FlowId, cdc::Cdc, change::Change},
};

#[derive(Clone, Debug)]
pub struct FlowInstruction {
	pub flow_id: FlowId,

	pub to_version: CommitVersion,

	pub changes: Vec<Change>,
}

impl FlowInstruction {
	pub fn new(flow_id: FlowId, to_version: CommitVersion, changes: Vec<Change>) -> Self {
		Self {
			flow_id,
			to_version,
			changes,
		}
	}
}

#[derive(Clone, Debug)]
pub struct WorkerBatch {
	pub state_version: CommitVersion,

	pub instructions: Vec<FlowInstruction>,
}

impl WorkerBatch {
	pub fn new(state_version: CommitVersion) -> Self {
		Self {
			state_version,
			instructions: Vec::new(),
		}
	}

	pub fn add_instruction(&mut self, instruction: FlowInstruction) {
		self.instructions.push(instruction);
	}
}

pub enum FlowResponse {
	Success {
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
	},

	Error(String),
}

pub type FlowHandle = ActorHandle<FlowMessage>;

pub enum FlowMessage {
	Process {
		batch: WorkerBatch,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},

	Register {
		flow_id: FlowId,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},

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

pub enum PoolResponse {
	Success {
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
	},

	RegisterSuccess,

	Error(String),
}

pub type FlowPoolHandle = ActorHandle<FlowPoolMessage>;

pub enum FlowPoolMessage {
	RegisterFlow {
		flow_id: FlowId,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},

	Submit {
		batches: BTreeMap<usize, WorkerBatch>,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},

	SubmitToWorker {
		worker_id: usize,
		batch: WorkerBatch,
		reply: Box<dyn FnOnce(PoolResponse) + Send>,
	},

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

	WorkerReply {
		worker_id: usize,
		response: FlowResponse,
	},
}

pub type FlowCoordinatorHandle = ActorHandle<FlowCoordinatorMessage>;

pub enum FlowCoordinatorMessage {
	Consume {
		cdcs: Vec<Cdc>,
		current_version: CommitVersion,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	},

	PoolReply(PoolResponse),

	Tick,
}
