// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_store_multi::flush::engine::FlushEngine;
use reifydb_value::value::duration::Duration;
use tracing::instrument;

pub struct PersistentFlushTask {
	engine: Arc<FlushEngine>,
	config: Arc<dyn GetConfig>,
	interval: Duration,
}

impl PersistentFlushTask {
	pub fn new(engine: Arc<FlushEngine>, config: Arc<dyn GetConfig>, interval: Duration) -> Self {
		Self {
			engine,
			config,
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

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::PersistentFlush]
	}

	#[instrument(name = "lifecycle::store::flush::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		let budget = (self.config.get_config_uint8(ConfigKey::MultiFlushKeyBudget) as usize).max(1);
		self.engine.sweep_slice(budget)
	}
}
