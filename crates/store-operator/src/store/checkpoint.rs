// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::FlowId, operator::FlowCheckpoint},
};
use reifydb_wal::lsn::Lsn;
use tracing::instrument;

use crate::{
	error::{OperatorError, Result},
	persistent::Checkpoint,
	store::{OperatorStore, StandardOperatorStore},
};

impl StandardOperatorStore {
	#[instrument(name = "store::operator::checkpoint_set", level = "debug", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_set(&self, flow: FlowId, checkpoint: FlowCheckpoint) -> Result<()> {
		if let Some(current) = self.checkpoint_get(flow)?
			&& checkpoint.version < current.version
		{
			return Err(OperatorError::CheckpointOutOfRange {
				flow,
			});
		}
		self.resident.record_checkpoint_set(flow, checkpoint);
		Ok(())
	}

	#[instrument(name = "store::operator::check_checkpoints", level = "trace", skip_all, fields(checkpoint_count = checkpoints.len()))]
	pub fn check_checkpoints(&self, checkpoints: &[(FlowId, FlowCheckpoint)]) -> Result<()> {
		for (flow, checkpoint) in checkpoints {
			if let Some(current) = self.checkpoint_get(*flow)?
				&& checkpoint.version < current.version
			{
				return Err(OperatorError::CheckpointOutOfRange {
					flow: *flow,
				});
			}
		}
		Ok(())
	}

	#[instrument(name = "store::operator::checkpoint_remove", level = "debug", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_remove(&self, flow: FlowId) -> Result<()> {
		self.resident.record_checkpoint_delete(flow);
		Ok(())
	}

	#[instrument(name = "store::operator::checkpoint_get", level = "trace", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_get(&self, flow: FlowId) -> Result<Option<FlowCheckpoint>> {
		if let Some(entry) = self.resident.lookup_checkpoint(flow) {
			return Ok(entry);
		}
		self.persistent.checkpoint_get(flow)
	}

	#[instrument(name = "store::operator::checkpoint_floor", level = "trace", skip(self))]
	pub fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		let pinned = self.pins.floor();
		let buffered = self.resident.checkpoint_floor();
		self.checkpoint_interlock();
		let durable = self.persistent.checkpoint_floor()?;
		Ok([pinned, buffered, durable].into_iter().flatten().min())
	}

	#[instrument(name = "store::operator::checkpoint_lsn_floor", level = "trace", skip(self))]
	pub fn checkpoint_lsn_floor(&self) -> Result<Option<Lsn>> {
		let buffered = self.resident.checkpoint_lsn_floor();
		self.checkpoint_interlock();
		let durable = self.persistent.checkpoint_lsn_floor()?;
		Ok([buffered, durable].into_iter().flatten().min())
	}

	#[instrument(name = "store::operator::checkpoint_list", level = "trace", skip(self))]
	pub fn checkpoint_list(&self) -> Result<Vec<FlowId>> {
		let buffered = self.resident.checkpoint_entries();
		self.checkpoint_interlock();
		let mut merged: BTreeSet<FlowId> = self.persistent.checkpoint_list()?.into_iter().collect();
		for (flow, entry) in buffered {
			match entry {
				Some(_) => merged.insert(flow),
				None => merged.remove(&flow),
			};
		}
		Ok(merged.into_iter().collect())
	}
}

impl OperatorStore {
	pub fn checkpoint_set(&self, flow: FlowId, checkpoint: FlowCheckpoint) -> Result<()> {
		match self {
			Self::Standard(store) => store.checkpoint_set(flow, checkpoint),
		}
	}

	pub fn check_checkpoints(&self, checkpoints: &[(FlowId, FlowCheckpoint)]) -> Result<()> {
		match self {
			Self::Standard(store) => store.check_checkpoints(checkpoints),
		}
	}

	pub fn checkpoint_remove(&self, flow: FlowId) -> Result<()> {
		match self {
			Self::Standard(store) => store.checkpoint_remove(flow),
		}
	}

	pub fn checkpoint_get(&self, flow: FlowId) -> Result<Option<FlowCheckpoint>> {
		match self {
			Self::Standard(store) => store.checkpoint_get(flow),
		}
	}

	pub fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		match self {
			Self::Standard(store) => store.checkpoint_floor(),
		}
	}

	pub fn checkpoint_lsn_floor(&self) -> Result<Option<Lsn>> {
		match self {
			Self::Standard(store) => store.checkpoint_lsn_floor(),
		}
	}

	pub fn checkpoint_list(&self) -> Result<Vec<FlowId>> {
		match self {
			Self::Standard(store) => store.checkpoint_list(),
		}
	}
}
