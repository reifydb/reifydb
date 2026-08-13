// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{
		class::{Floor, FloorTerm, RetentionClass},
		progress::Progress,
		task::LifecycleTask,
	},
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
		let batch_size = (self.config.get_config_uint8(ConfigKey::TombstoneReapBatchSize) as usize).max(1);

		let kinds = match persistent.list_current_entries() {
			Ok(kinds) => kinds,
			Err(e) => {
				warn!(error = %e, "tombstone reaper failed to list persistent entries");
				self.plane.record_reclamation(RetentionClass::TombstoneReap, None, 0, 0);
				return Progress::Exhausted;
			}
		};

		let mut reaped = 0u64;
		let mut backlog = 0u64;
		let mut lowest: Option<(Floor, FloorTerm)> = None;
		for kind in kinds {
			let Some(floor) =
				self.plane.kind_cutoff_with_binding(RetentionClass::TombstoneReap, kind, now, None)
			else {
				continue;
			};
			let Some(cutoff) = floor.0.version() else {
				continue;
			};
			lowest = Some(match lowest {
				Some(current) if current.0.monotonic_key() <= floor.0.monotonic_key() => current,
				_ => floor,
			});
			match persistent.reap_tombstones(kind, cutoff, batch_size) {
				Ok((count, more)) => {
					reaped += count;
					if more {
						backlog += 1;
					}
				}
				Err(e) => {
					warn!(kind = ?kind, error = %e, "tombstone reaper failed on an entry");
				}
			}
		}

		self.plane.record_reclamation(RetentionClass::TombstoneReap, lowest, reaped, backlog);
		if backlog > 0 {
			self.plane.record_budget_exhausted(RetentionClass::TombstoneReap);
			Progress::Yielded
		} else {
			Progress::Exhausted
		}
	}
}
