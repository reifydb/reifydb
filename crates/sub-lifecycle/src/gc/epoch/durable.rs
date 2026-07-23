// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Durable version-epoch log.
//!
//! Every TTL in the system resolves through the epoch: a cutoff version is `floor_version_at(now - ttl)`. While the
//! epoch lived only in RAM, that lookup returned none for any instant before process start, so after every restart
//! nothing expired until the process had been up longer than the TTL itself - and at production commit rates the
//! bounded RAM sample buffer meant long TTLs never resolved even without a restart. Both are the same defect:
//! reclamation that reports success while deleting nothing.
//!
//! This task persists one (bucket, version) sample per wall-clock bucket into the existing VersionEpoch keyspace,
//! prunes samples older than the retention horizon, and hydrates the RAM map at boot. Coverage then depends on the
//! horizon, not on uptime.
//!
//! Two guards are load-bearing. Sampling skips when the head version has not moved, so an idle database does not
//! allocate a version per bucket forever - each sample commit would otherwise be the very change the next sample
//! records. And pruning is gated and budgeted like every other reclaimer, because at boot the whole backlog is
//! eligible at once.
//!
//! Pruning removes rather than drops: a drop collapses a key to its newest version, which for a one-version-per-
//! bucket log removes nothing. The resulting persistent-tier tombstones are reaped by the tombstone class, the same
//! path every other delete-mode expiry relies on. This keyspace is CDC-excluded, so the removal emits no change
//! event and no dependent flow observes it.

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
	key::{EncodableKey, version_epoch::VersionEpochKey},
	lifecycle::{gate::RetentionStartupGate, progress::Progress, task::LifecycleTask},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::version_epoch::VersionEpoch;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId, value_type::ValueType},
};
use tracing::{debug, instrument, warn};

const PRUNE_BUDGET: usize = 1024;

const RANGE_BATCH: usize = 256;

fn sample_shape() -> RowShape {
	RowShape::testing(&[ValueType::Uint8])
}

fn encode_version(shape: &RowShape, version: CommitVersion) -> EncodedRow {
	let mut row = shape.allocate();
	shape.set_u64(&mut row, 0, version.0);
	row
}

fn bucket_of(now_nanos: u64, bucket_nanos: u64) -> u64 {
	now_nanos - (now_nanos % bucket_nanos)
}

fn bucket_nanos(catalog: &Catalog) -> u64 {
	let configured = catalog.get_config_duration(ConfigKey::EpochBucketDuration);
	match configured.as_nanos() {
		Ok(nanos) if nanos > 0 => nanos as u64,
		_ => 60_000_000_000,
	}
}

#[instrument(name = "lifecycle::gc::epoch::hydrate", level = "debug", skip_all)]
pub fn hydrate(engine: &StandardEngine, horizon: Duration) -> Result<usize> {
	let now_nanos = engine.clock().now_nanos();
	let oldest_covered = DateTime::from_nanos(now_nanos).checked_sub(horizon).map(|t| t.to_nanos()).unwrap_or(0);

	let shape = sample_shape();
	let mut samples: Vec<(u64, u64)> = Vec::new();

	let txn = engine.begin_query(IdentityId::system())?;
	for entry in txn.range(VersionEpochKey::floor_scan(now_nanos), RangeScope::All, RANGE_BATCH) {
		let entry = entry?;
		let Some(key) = VersionEpochKey::decode(&entry.key) else {
			continue;
		};
		if key.bucket_nanos < oldest_covered {
			break;
		}
		samples.push((key.bucket_nanos, shape.get_u64(&entry.row, 0)));
	}

	samples.sort_unstable();
	samples.dedup_by_key(|(bucket, _)| *bucket);

	let epoch = engine.version_epoch();
	for (bucket, version) in &samples {
		epoch.record(*bucket, *version);
	}

	if !samples.is_empty() {
		debug!(
			samples = samples.len(),
			oldest_bucket = samples.first().map(|(b, _)| *b),
			"version epoch hydrated from durable samples"
		);
	}

	Ok(samples.len())
}

pub struct EpochLogTask {
	engine: StandardEngine,
	catalog: Catalog,
	gate: RetentionStartupGate,
	shape: RowShape,
	last_sample: Option<(u64, u64)>,
}

impl EpochLogTask {
	pub fn new(engine: StandardEngine, gate: RetentionStartupGate) -> Self {
		let catalog = engine.catalog();
		Self {
			engine,
			catalog,
			gate,
			shape: sample_shape(),
			last_sample: None,
		}
	}

	fn epoch(&self) -> &VersionEpoch {
		self.engine.version_epoch()
	}

	#[instrument(name = "lifecycle::gc::epoch::persist", level = "debug", skip_all)]
	fn persist_sample(&mut self, now_nanos: u64) -> Result<bool> {
		let bucket = bucket_of(now_nanos, bucket_nanos(&self.catalog));
		let version = self.engine.current_version()?;
		if version.0 == 0 {
			return Ok(false);
		}

		if let Some((last_bucket, settled_at)) = self.last_sample
			&& last_bucket == bucket
			&& version.0 <= settled_at
		{
			return Ok(false);
		}

		let key = VersionEpochKey::encoded(bucket);
		let row = encode_version(&self.shape, version);

		let mut txn = self.engine.begin_command(IdentityId::system())?;
		txn.set(&key, row)?;
		let written_at = txn.commit_unchecked()?;

		self.epoch().record(bucket, version.0);
		self.last_sample = Some((bucket, written_at.0.max(version.0)));
		Ok(true)
	}

	#[instrument(name = "lifecycle::gc::epoch::prune", level = "debug", skip_all)]
	fn prune(&mut self, now_nanos: u64) -> Result<Progress> {
		let horizon = crate::plane::horizon::max_retention_horizon(&self.catalog);
		let Some(cutoff) = DateTime::from_nanos(now_nanos).checked_sub(horizon) else {
			return Ok(Progress::Exhausted);
		};

		let mut expired: Vec<EncodedKey> = Vec::new();
		{
			let txn = self.engine.begin_query(IdentityId::system())?;
			for entry in txn.range(VersionEpochKey::older_than(cutoff.to_nanos()), RangeScope::All, RANGE_BATCH) {
				let entry = entry?;
				expired.push(entry.key);
				if expired.len() >= PRUNE_BUDGET {
					break;
				}
			}
		}

		if expired.is_empty() {
			return Ok(Progress::Exhausted);
		}

		let drained = expired.len();
		let mut txn = self.engine.begin_command(IdentityId::system())?;
		for key in expired {
			txn.remove(&key)?;
		}
		txn.commit_unchecked()?;

		debug!(pruned = drained, cutoff = cutoff.to_nanos(), "pruned epoch samples beyond the retention horizon");

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
		self.catalog.get_config_duration(ConfigKey::EpochBucketDuration)
	}

	#[instrument(name = "lifecycle::gc::epoch::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		let now_nanos = self.engine.clock().now_nanos();

		if let Err(e) = self.persist_sample(now_nanos) {
			warn!(error = %e, "durable version-epoch sample failed; retrying next slice");
			return Progress::Exhausted;
		}

		if !self.gate.is_open() {
			self.gate.record_skip();
			return Progress::Exhausted;
		}

		match self.prune(now_nanos) {
			Ok(progress) => progress,
			Err(e) => {
				warn!(error = %e, "epoch sample pruning failed; retrying next slice");
				Progress::Exhausted
			}
		}
	}
}
