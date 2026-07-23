// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	event::lifecycle::VersionEpochSampledEvent,
	interface::{
		WithEventBus,
		catalog::config::{ConfigKey, GetConfig},
	},
	lifecycle::{class::RetentionClass, gate::RetentionStartupGate, progress::Progress, task::LifecycleTask},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::version_epoch::VersionEpoch;
use reifydb_value::{
	Result,
	count::Count,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::{debug, instrument, warn};

use crate::{gc::epoch::log::EpochLog, plane::horizon::max_retention_horizon};

const PRUNE_BUDGET: usize = 1024;

#[instrument(name = "lifecycle::gc::epoch::hydrate", level = "debug", skip_all)]
pub fn hydrate(engine: &StandardEngine, horizon: Duration) -> Result<usize> {
	hydrate_into(engine, horizon, engine.version_epoch())
}

pub fn hydrate_into(engine: &StandardEngine, horizon: Duration, epoch: &VersionEpoch) -> Result<usize> {
	let now_nanos = engine.clock().now_nanos();
	let oldest_covered = DateTime::from_nanos(now_nanos).checked_sub(horizon).map(|t| t.to_nanos()).unwrap_or(0);

	let samples = EpochLog::new(engine.clone()).read_since(oldest_covered, now_nanos)?;
	for sample in &samples {
		epoch.backfill(sample.at_nanos, sample.version);
	}

	if !samples.is_empty() {
		debug!(
			samples = samples.len(),
			oldest = samples.first().map(|sample| sample.at_nanos),
			"version epoch hydrated from durable samples"
		);
	}

	Ok(samples.len())
}

pub struct EpochLogTask {
	engine: StandardEngine,
	catalog: Catalog,
	gate: RetentionStartupGate,
	log: EpochLog,
	pruned_since_emit: Count,
}

impl EpochLogTask {
	pub fn new(engine: StandardEngine, gate: RetentionStartupGate) -> Self {
		let catalog = engine.catalog();
		Self {
			log: EpochLog::new(engine.clone()),
			engine,
			catalog,
			gate,
			pruned_since_emit: Count::ZERO,
		}
	}

	fn epoch(&self) -> &VersionEpoch {
		self.engine.version_epoch()
	}

	#[instrument(name = "lifecycle::gc::epoch::persist", level = "debug", skip_all)]
	fn persist_sample(&mut self, now_nanos: u64) -> Result<bool> {
		let version = self.engine.current_version()?;
		if !self.log.write(now_nanos, version)? {
			return Ok(false);
		}
		self.epoch().backfill(now_nanos, version.0);
		Ok(true)
	}

	fn emit_sampled(&mut self) {
		let durable_samples = match self.log.durable_count() {
			Ok(count) => Count::new(count),
			Err(e) => {
				warn!(error = %e, "durable epoch sample count failed; skipping this emit");
				return;
			}
		};
		self.engine.event_bus().emit(VersionEpochSampledEvent::new(durable_samples, self.pruned_since_emit));
		self.pruned_since_emit = Count::ZERO;
	}

	#[instrument(name = "lifecycle::gc::epoch::prune", level = "debug", skip_all)]
	fn prune(&mut self, now_nanos: u64) -> Result<Progress> {
		let horizon = max_retention_horizon(&self.catalog);
		let Some(cutoff) = DateTime::from_nanos(now_nanos).checked_sub(horizon) else {
			return Ok(Progress::Exhausted);
		};

		let expired = self.log.expired_before(cutoff.to_nanos(), PRUNE_BUDGET)?;

		if expired.is_empty() {
			return Ok(Progress::Exhausted);
		}

		let drained = expired.len();
		let mut txn = self.engine.begin_command(IdentityId::system())?;
		for key in expired {
			txn.remove(&key)?;
		}
		txn.commit_unchecked()?;

		self.pruned_since_emit = self.pruned_since_emit.saturating_add(Count::new(drained as u64));
		debug!(
			pruned = drained,
			cutoff = cutoff.to_nanos(),
			"pruned epoch samples beyond the retention horizon"
		);

		if drained >= PRUNE_BUDGET {
			Ok(Progress::Yielded)
		} else {
			Ok(Progress::Exhausted)
		}
	}
}

impl LifecycleTask for EpochLogTask {
	fn name(&self) -> &'static str {
		"epoch-log"
	}

	fn interval(&self) -> Duration {
		self.catalog.get_config_duration(ConfigKey::EpochBucketInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::EpochLog]
	}

	#[instrument(name = "lifecycle::gc::epoch::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		let now_nanos = self.engine.clock().now_nanos();

		let stats = self.engine.version_epoch().stats();
		debug!(
			samples = stats.samples,
			coverage_seconds = stats.coverage_nanos / 1_000_000_000,
			floor_none_returns = stats.floor_none_returns,
			"version epoch coverage"
		);

		let persisted = match self.persist_sample(now_nanos) {
			Ok(persisted) => persisted,
			Err(e) => {
				warn!(error = %e, "durable version-epoch sample failed; retrying next slice");
				return Progress::Exhausted;
			}
		};

		let progress = if self.gate.is_open() {
			match self.prune(now_nanos) {
				Ok(progress) => progress,
				Err(e) => {
					warn!(error = %e, "epoch sample pruning failed; retrying next slice");
					Progress::Exhausted
				}
			}
		} else {
			self.gate.record_skip();
			Progress::Exhausted
		};

		if persisted {
			self.emit_sampled();
		}
		progress
	}
}
