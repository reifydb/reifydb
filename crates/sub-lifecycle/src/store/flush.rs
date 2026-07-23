// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::lifecycle::{progress::Progress, task::LifecycleTask};
use reifydb_store_multi::flush::engine::{FLUSH_KEY_BUDGET, FlushEngine};
use reifydb_value::value::duration::Duration;
use tracing::instrument;

pub struct PersistentFlushTask {
	engine: Arc<FlushEngine>,
	interval: Duration,
}

impl PersistentFlushTask {
	pub fn new(engine: Arc<FlushEngine>, interval: Duration) -> Self {
		Self {
			engine,
			interval,
		}
	}
}

impl LifecycleTask for PersistentFlushTask {
	fn name(&self) -> &'static str {
		"persistent-flush"
	}

	fn interval(&self) -> Duration {
		self.interval
	}

	#[instrument(name = "lifecycle::store::flush::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		self.engine.sweep_slice(FLUSH_KEY_BUDGET)
	}
}
