// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use tracing::instrument;

use crate::{
	error::Result,
	persistent::{Checkpoint, memory::MemoryPersistent},
};

impl Checkpoint for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::checkpoint_get", level = "trace", skip_all)]
	fn checkpoint_get(&self, flow: FlowId) -> Result<Option<CommitVersion>> {
		Ok(self.0.checkpoints.lock().get(&flow).copied())
	}

	#[instrument(name = "store::operator::persistent::memory::checkpoint_set", level = "trace", skip_all)]
	fn checkpoint_set(&self, flow: FlowId, version: CommitVersion) -> Result<()> {
		self.0.checkpoints.lock().insert(flow, version);
		Ok(())
	}

	#[instrument(name = "store::operator::persistent::memory::checkpoint_remove", level = "trace", skip_all)]
	fn checkpoint_remove(&self, flow: FlowId) -> Result<()> {
		self.0.checkpoints.lock().remove(&flow);
		Ok(())
	}

	#[instrument(name = "store::operator::persistent::memory::checkpoint_floor", level = "trace", skip_all)]
	fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		Ok(self.0.checkpoints.lock().values().copied().min())
	}

	#[instrument(name = "store::operator::persistent::memory::checkpoint_list", level = "trace", skip_all)]
	fn checkpoint_list(&self) -> Result<Vec<FlowId>> {
		Ok(self.0.checkpoints.lock().keys().copied().collect())
	}
}
