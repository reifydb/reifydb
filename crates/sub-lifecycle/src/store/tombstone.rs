// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::value::duration::Duration;
use tracing::{instrument, warn};

use crate::plane::RetentionPlane;

pub struct TombstoneReapTask {
	store: StandardMultiStore,
	plane: RetentionPlane,
	clock: Clock,
	config: Arc<dyn GetConfig>,
}

impl TombstoneReapTask {
	pub fn new(store: StandardMultiStore, plane: RetentionPlane, clock: Clock, config: Arc<dyn GetConfig>) -> Self {
		Self {
			store,
			plane,
			clock,
			config,
		}
	}
}

impl LifecycleTask for TombstoneReapTask {
	fn name(&self) -> &'static str {
		"tombstone-reap"
	}

	fn interval(&self) -> Duration {
		self.config.get_config_duration(ConfigKey::TombstoneReapInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::TombstoneReap]
	}

	#[instrument(name = "lifecycle::store::tombstone::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		let now = self.clock.now();
		let Some(persistent) = self.store.persistent() else {
			return Progress::Exhausted;
		};
		let Some(floor) = self.plane.cutoff_with_binding(RetentionClass::TombstoneReap, now, None) else {
			self.plane.record_reclamation(RetentionClass::TombstoneReap, None, 0, 0);
			return Progress::Exhausted;
		};
		let cutoff = floor.0;
		let batch_size = (self.config.get_config_uint8(ConfigKey::TombstoneReapBatchSize) as usize).max(1);

		let tables = match persistent.list_current_table_names() {
			Ok(tables) => tables,
			Err(e) => {
				warn!(error = %e, "tombstone reaper failed to list persistent tables");
				self.plane.record_reclamation(RetentionClass::TombstoneReap, Some(floor), 0, 0);
				return Progress::Exhausted;
			}
		};

		let mut reaped = 0u64;
		let mut backlog = 0u64;
		for table in &tables {
			match persistent.reap_tombstones(table, cutoff, batch_size) {
				Ok((count, more)) => {
					reaped += count;
					if more {
						backlog += 1;
					}
				}
				Err(e) => {
					warn!(table = %table, error = %e, "tombstone reaper failed on a table");
				}
			}
		}

		self.plane.record_reclamation(RetentionClass::TombstoneReap, Some(floor), reaped, backlog);
		if backlog > 0 {
			self.plane.record_budget_exhausted(RetentionClass::TombstoneReap);
			Progress::Yielded
		} else {
			Progress::Exhausted
		}
	}
}
