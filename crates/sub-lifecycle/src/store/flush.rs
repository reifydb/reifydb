// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_multi::tier::commit::domain::MultiCommitTier;
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};
use tracing::instrument;

use crate::plane::RetentionPlane;

pub struct PersistentFlushTask {
	tier: MultiCommitTier,
	config: Arc<dyn GetConfig>,
	plane: RetentionPlane,
	clock: Clock,
	interval: Duration,
}

impl PersistentFlushTask {
	pub fn new(
		tier: MultiCommitTier,
		config: Arc<dyn GetConfig>,
		plane: RetentionPlane,
		clock: Clock,
		interval: Duration,
	) -> Self {
		Self {
			tier,
			config,
			plane,
			clock,
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
		let budget =
			ByteSize::from_bytes(self.config.get_config_uint8(ConfigKey::MultiFlushBudgetBytes).max(1));
		let now = self.clock.now();

		let Some(floor) = self.plane.cutoff_with_binding(RetentionClass::PersistentFlush, now, None) else {
			self.plane.record_reclamation(RetentionClass::PersistentFlush, None, 0, 0);
			return Progress::Exhausted;
		};

		let outcome = self.tier.flush_slice(budget);
		self.plane.record_reclamation(
			RetentionClass::PersistentFlush,
			Some(floor),
			outcome.reclaimed,
			self.tier.state().buffered_entries(),
		);

		if outcome.progress.is_yielded() {
			self.plane.record_budget_exhausted(RetentionClass::PersistentFlush);
		}
		outcome.progress
	}
}
