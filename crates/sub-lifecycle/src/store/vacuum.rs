// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::value::duration::Duration;
use tracing::{instrument, warn};

use crate::plane::RetentionPlane;

pub struct VacuumBudgetTask {
	store: StandardMultiStore,
	plane: RetentionPlane,
	config: Arc<dyn GetConfig>,
}

impl VacuumBudgetTask {
	pub fn new(store: StandardMultiStore, plane: RetentionPlane, config: Arc<dyn GetConfig>) -> Self {
		Self {
			store,
			plane,
			config,
		}
	}
}

impl LifecycleTask for VacuumBudgetTask {
	fn name(&self) -> &'static str {
		"vacuum-budget"
	}

	fn interval(&self) -> Duration {
		self.config.get_config_duration(ConfigKey::VacuumInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::VacuumBudget]
	}

	#[instrument(name = "lifecycle::store::vacuum::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		let Some(persistent) = self.store.persistent() else {
			return Progress::Exhausted;
		};
		let (freelist, pages) = match persistent.freelist_page_count() {
			Ok(counts) => counts,
			Err(e) => {
				warn!(error = %e, "vacuum task failed to read the persistent freelist");
				return Progress::Exhausted;
			}
		};

		let threshold = self.config.get_config_uint8(ConfigKey::VacuumFreelistThresholdPercent);
		if pages == 0 || freelist == 0 || freelist.saturating_mul(100) <= threshold.saturating_mul(pages) {
			self.plane.record_reclamation(RetentionClass::VacuumBudget, None, 0, 0);
			return Progress::Exhausted;
		}

		let per_slice = self.config.get_config_uint8(ConfigKey::VacuumPagesPerSlice).max(1);
		let moved = match persistent.incremental_vacuum(per_slice) {
			Ok(moved) => moved,
			Err(e) => {
				warn!(error = %e, "vacuum task failed to run incremental_vacuum");
				self.plane.record_reclamation(RetentionClass::VacuumBudget, None, 0, 0);
				return Progress::Exhausted;
			}
		};

		let (freelist_after, pages_after) = persistent.freelist_page_count().unwrap_or((0, 0));
		let still_over =
			pages_after > 0 && freelist_after.saturating_mul(100) > threshold.saturating_mul(pages_after);
		let more = moved > 0 && still_over;

		self.plane.record_reclamation(RetentionClass::VacuumBudget, None, moved, u64::from(more));
		if more {
			self.plane.record_budget_exhausted(RetentionClass::VacuumBudget);
			Progress::Yielded
		} else {
			Progress::Exhausted
		}
	}
}
