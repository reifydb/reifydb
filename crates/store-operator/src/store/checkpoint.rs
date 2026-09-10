// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use tracing::instrument;

use crate::{
	error::{OperatorError, Result},
	persistent::Checkpoint,
	store::{OperatorStore, StandardOperatorStore},
};

impl StandardOperatorStore {
	#[instrument(name = "store::operator::checkpoint_set", level = "debug", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) -> Result<()> {
		if let Some(current) = self.checkpoint_get(flow)?
			&& version < current
		{
			return Err(OperatorError::CheckpointOutOfRange {
				flow,
			});
		}
		self.resident.record_checkpoint_set(flow, version);
		Ok(())
	}

	#[instrument(name = "store::operator::checkpoint_remove", level = "debug", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_remove(&self, flow: FlowId) -> Result<()> {
		self.resident.record_checkpoint_delete(flow);
		Ok(())
	}

	#[instrument(name = "store::operator::checkpoint_get", level = "trace", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_get(&self, flow: FlowId) -> Result<Option<CommitVersion>> {
		if let Some(entry) = self.resident.lookup_checkpoint(flow) {
			return Ok(entry);
		}
		self.persistent.checkpoint_get(flow)
	}

	#[instrument(name = "store::operator::checkpoint_floor", level = "trace", skip(self))]
	pub fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		let buffered = self.resident.checkpoint_floor();
		self.checkpoint_interlock();
		let durable = self.persistent.checkpoint_floor()?;
		Ok(match (durable, buffered) {
			(Some(durable), Some(buffered)) => Some(durable.min(buffered)),
			(durable, buffered) => durable.or(buffered),
		})
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
	pub fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) -> Result<()> {
		match self {
			Self::Standard(store) => store.checkpoint_set(flow, version),
		}
	}

	pub fn checkpoint_remove(&self, flow: FlowId) -> Result<()> {
		match self {
			Self::Standard(store) => store.checkpoint_remove(flow),
		}
	}

	pub fn checkpoint_get(&self, flow: FlowId) -> Result<Option<CommitVersion>> {
		match self {
			Self::Standard(store) => store.checkpoint_get(flow),
		}
	}

	pub fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		match self {
			Self::Standard(store) => store.checkpoint_floor(),
		}
	}

	pub fn checkpoint_list(&self) -> Result<Vec<FlowId>> {
		match self {
			Self::Standard(store) => store.checkpoint_list(),
		}
	}
}
