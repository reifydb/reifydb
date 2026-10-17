// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::store::WalCheckpoint,
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_value::value::duration::Duration;

pub struct CommitLogCheckpointTask {
	checkpoint: Arc<dyn WalCheckpoint>,
	interval: Duration,
}

impl CommitLogCheckpointTask {
	pub fn new(checkpoint: Arc<dyn WalCheckpoint>, interval: Duration) -> Self {
		Self {
			checkpoint,
			interval,
		}
	}
}

impl LifecycleTask for CommitLogCheckpointTask {
	fn name(&self) -> &'static str {
		"commit-log-checkpoint"
	}

	fn interval(&self) -> Duration {
		self.interval
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[]
	}

	fn run_slice(&mut self) -> Progress {
		let version = self.checkpoint.make_durable().expect("commit log checkpoint failed to flush the log");
		self.checkpoint
			.saved_through(version)
			.expect("commit log checkpoint failed to advance the multi floor");
		Progress::Exhausted
	}
}
