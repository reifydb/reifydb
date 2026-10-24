// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::value::duration::Duration;
use tracing::instrument;

pub struct CommitMergeTask {
	store: StandardMultiStore,
	config: Arc<dyn GetConfig>,
}

impl CommitMergeTask {
	pub fn new(store: StandardMultiStore, config: Arc<dyn GetConfig>) -> Self {
		Self {
			store,
			config,
		}
	}
}

impl LifecycleTask for CommitMergeTask {
	fn name(&self) -> &'static str {
		"commit-merge"
	}

	fn interval(&self) -> Duration {
		self.config.get_config_duration(ConfigKey::CommitMergeInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::CommitMerge]
	}

	#[instrument(name = "lifecycle::store::merge::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		self.store.commit().merge_closed();
		Progress::Exhausted
	}
}
