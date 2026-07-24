// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask};
use reifydb_store_multi::store::worker::CompactionEngine;
use reifydb_value::value::duration::Duration;
use tracing::instrument;

pub struct CompactionReclaimTask {
	engine: Arc<CompactionEngine>,
	interval: Duration,
}

impl CompactionReclaimTask {
	pub fn new(engine: Arc<CompactionEngine>, interval: Duration) -> Self {
		Self {
			engine,
			interval,
		}
	}
}

impl LifecycleTask for CompactionReclaimTask {
	fn name(&self) -> &'static str {
		"compaction-reclaim"
	}

	fn interval(&self) -> Duration {
		self.interval
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::CompactionReclaim]
	}

	#[instrument(name = "lifecycle::store::compaction::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		self.engine.drain_slice(self.engine.drain_budget())
	}
}
