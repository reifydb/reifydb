// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask};
use reifydb_store_multi::store::worker::DropEngine;
use reifydb_value::value::duration::Duration;
use tracing::instrument;

pub struct DropReclaimTask {
	engine: Arc<DropEngine>,
	interval: Duration,
}

impl DropReclaimTask {
	pub fn new(engine: Arc<DropEngine>, interval: Duration) -> Self {
		Self {
			engine,
			interval,
		}
	}
}

impl LifecycleTask for DropReclaimTask {
	fn name(&self) -> &'static str {
		"drop-reclaim"
	}

	fn interval(&self) -> Duration {
		self.interval
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::PendingDropsPurge]
	}

	#[instrument(name = "lifecycle::store::drop::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		self.engine.drain_slice(self.engine.drain_budget())
	}
}
