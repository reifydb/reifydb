// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
//! Durable version-epoch log: what keeps the time-to-version map answerable across a restart.
//!
//! No row or operator ttl resolves through this map any more - both compare a row's own timestamp against a cutoff
//! instant. These tests pin the map itself: samples reaching storage, hydration covering instants older than this
//! process, and pruning bounded by the longest declared ttl rather than by a buffer size.

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		cdc::{CdcConsumerId, ConsumerClass},
	},
	key::{EncodableKey, version_epoch::VersionEpochKey},
	lifecycle::{gate::RetentionStartupGate, progress::Progress, task::LifecycleTask},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	context::clock::Clock,
	version_epoch::{EpochSeconds, VersionEpoch},
};
use reifydb_sub_lifecycle::gc::epoch::{
	durable::{EpochLogTask, hydrate, hydrate_into},
	log::EpochLog,
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::value::{duration::Duration, identity::IdentityId, value_type::ValueType};

const MINUTE_SECS: u64 = 60;

/// Advances the head commit version; the epoch only records a sample once the head has moved.
fn commit_a_version(engine: &StandardEngine, consumer: &str, at: u64) {
	let mut txn = engine.begin_command(IdentityId::system()).expect("system command transaction");
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new(consumer), CommitVersion(at), ConsumerClass::Ephemeral)
		.expect("checkpoint write");
	txn.commit().expect("commit");
}

/// Reads the durable keyspace directly, so a test asserts what reached storage rather than what the RAM map
/// still holds. Returns (bucket, sample instant, version); bucket and instant diverging is the thing under test.
fn durable_samples(engine: &StandardEngine) -> Vec<(u64, u64, u64)> {
	let shape = RowShape::testing(&[ValueType::Uint8, ValueType::Uint8]);
	let txn = engine.begin_query(IdentityId::system()).expect("system query transaction");
	let mut samples: Vec<(u64, u64, u64)> = txn
		.range(VersionEpochKey::floor_scan(EpochSeconds::new(u64::MAX)), RangeScope::All, 256)
		.filter_map(|entry| {
			let entry = entry.ok()?;
			let key = VersionEpochKey::decode(&entry.key)?;
			Some((key.bucket.seconds(), shape.get::<u64>(&entry.row, 0), shape.get::<u64>(&entry.row, 1)))
		})
		.collect();
	samples.sort_unstable();
	samples
}

/// The CDC harness registers a listener that warms the shared epoch, which can satisfy an assertion the code
/// under test never satisfied.
fn engine_without_epoch_listener() -> TestEngine {
	TestEngine::builder().build()
}

fn open_gate(engine: &StandardEngine) -> RetentionStartupGate {
	RetentionStartupGate::open(engine.clock().clone())
}

#[test]
fn a_sample_reaches_storage_so_a_later_process_can_read_it() {
	// A sample that only ever lives in RAM resets the map's coverage to zero on every restart.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();
	commit_a_version(&engine, "ingest", 1);

	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	task.run_slice();

	let samples = durable_samples(&engine);
	assert_eq!(samples.len(), 1, "one slice must persist exactly one sample; got {samples:?}");
	assert!(samples[0].1 > 0, "a persisted sample must carry the head commit version, not zero");
}

#[test]
fn hydration_restores_coverage_for_an_instant_that_precedes_this_process() {
	// An instant from before this process started must still resolve to a version, or the map answers nothing
	// about anything older than the current uptime. Hydration targets a fresh epoch so the assertion cannot
	// pass off the shared map.
	let t = engine_without_epoch_listener();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();

	commit_a_version(&engine, "ingest", 1);
	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	let early_version = engine.current_version().expect("head version");
	task.run_slice();
	let early_instant = engine.clock().now().to_secs();

	clock.advance_hours(6);
	commit_a_version(&engine, "ingest", 2);
	task.run_slice();

	// A cold map stands in for the restarted process.
	let restored_epoch = VersionEpoch::new();
	assert_eq!(
		restored_epoch.floor_version_at(EpochSeconds::new(early_instant)),
		None,
		"precondition: a cold epoch resolves nothing, which is why a restart used to stop reclaiming"
	);

	let restored = hydrate_into(&engine, Duration::from_days(7).unwrap(), &restored_epoch)
		.expect("hydration reads the durable log");

	assert!(restored >= 2, "hydration must find every sample inside the horizon; found {restored}");
	assert_eq!(
		restored_epoch.floor_version_at(EpochSeconds::new(early_instant)),
		Some(early_version.0),
		"the instant of the first sample must resolve to the version that was current then, restored \
		 entirely from disk"
	);
}

#[test]
fn hydration_restores_history_even_though_the_sampler_already_recorded_now() {
	// The sampler records the current instant before hydration runs, so every durable sample is older than what
	// the map already holds; a forward-only insert would discard all of them and leave the restarted process
	// with no coverage at all, silently.
	let t = engine_without_epoch_listener();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();

	commit_a_version(&engine, "ingest", 1);
	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	let early_version = engine.current_version().expect("head version");
	task.run_slice();
	let early_instant = engine.clock().now().to_secs();

	clock.advance_hours(6);
	commit_a_version(&engine, "ingest", 2);
	task.run_slice();

	let restored_epoch = VersionEpoch::new();
	let boot_instant = engine.clock().now().to_secs();
	let boot_version = engine.current_version().expect("head version");
	restored_epoch.record(EpochSeconds::new(boot_instant), boot_version.0);

	hydrate_into(&engine, Duration::from_days(7).unwrap(), &restored_epoch)
		.expect("hydration reads the durable log");

	assert_eq!(
		restored_epoch.floor_version_at(EpochSeconds::new(early_instant)),
		Some(early_version.0),
		"a sample already taken for the current instant must not block restoring older history"
	);
}

#[test]
fn a_sample_never_claims_a_version_was_current_before_it_was() {
	// Filing a mid-bucket sample under the bucket start claims that version was reached up to a bucket earlier,
	// so any reader of the map gets a floor newer than the truth for every instant in that bucket.
	let t = engine_without_epoch_listener();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();

	commit_a_version(&engine, "ingest", 1);
	let early_version = engine.current_version().expect("head version");
	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));

	// Move well inside the bucket and commit again, so the head at sample time is strictly ahead of the head
	// at the bucket start.
	clock.advance_secs(45);
	commit_a_version(&engine, "ingest", 2);
	task.run_slice();

	let samples = durable_samples(&engine);
	assert_eq!(samples.len(), 1, "one slice persists one sample; got {samples:?}");
	let (bucket, at, version) = samples[0];
	assert!(at > bucket, "the sample instant must be recorded, not collapsed onto the bucket start");
	assert!(version > early_version.0, "precondition: the head advanced inside the bucket");

	let restored = VersionEpoch::new();
	hydrate_into(&engine, Duration::from_days(7).unwrap(), &restored).expect("hydration reads the durable log");

	assert_ne!(
		restored.floor_version_at(EpochSeconds::new(bucket)),
		Some(version),
		"the bucket start must not resolve to a version only reached later in the bucket"
	);
}

#[test]
fn a_sample_at_the_horizon_edge_survives_because_its_instant_is_still_covered() {
	// A row filed under a bucket that has just left the window can still hold a sample taken inside it.
	// Deciding by bucket drops it, shortening coverage by up to one bucket at the oldest edge of the horizon,
	// and an uncovered instant resolves to nothing at all.
	let t = engine_without_epoch_listener();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();

	commit_a_version(&engine, "ingest", 1);
	clock.advance_secs(45);
	commit_a_version(&engine, "ingest", 2);
	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	task.run_slice();

	let (bucket, at, version) = durable_samples(&engine)[0];
	let now = engine.clock().now().to_secs();

	// A horizon that lands between the bucket start and the sample instant: the bucket is outside, the sample
	// is not.
	let horizon = Duration::from_seconds((now - bucket) as i64 - 1).unwrap();
	assert!(now - horizon.to_std().as_secs() > bucket, "precondition: the bucket is outside the window");
	assert!(now - horizon.to_std().as_secs() <= at, "precondition: the sample instant is inside the window");

	let restored = VersionEpoch::new();
	hydrate_into(&engine, horizon, &restored).expect("hydration reads the durable log");

	assert_eq!(
		restored.floor_version_at(EpochSeconds::new(at)),
		Some(version),
		"a sample whose instant is inside the horizon must survive its bucket falling outside"
	);
}

#[test]
fn a_sample_is_not_pruned_while_its_instant_is_still_inside_the_horizon() {
	// The pruner's half of the same edge: deleting by bucket throws away a sample taken inside the window, and
	// the coverage it was providing goes with it.
	let t = engine_without_epoch_listener();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();

	commit_a_version(&engine, "ingest", 1);
	clock.advance_secs(45);
	commit_a_version(&engine, "ingest", 2);
	EpochLogTask::new(engine.clone(), open_gate(&engine)).run_slice();

	let (bucket, at, _version) = durable_samples(&engine)[0];
	assert!(at > bucket + 1, "precondition: the sample sits well inside its bucket");

	// A cutoff strictly after the bucket start but at or before the sample instant: the bucket is expired, the
	// sample is not.
	let cutoff = bucket + (at - bucket) / 2;
	let expired =
		EpochLog::new(engine.clone()).expired_before(EpochSeconds::new(cutoff), 1024).expect("prune scan");

	assert!(
		expired.is_empty(),
		"a sample taken at {at} must survive a cutoff of {cutoff}; its bucket {bucket} being older is not \
		 evidence the sample is"
	);
}

#[test]
fn an_idle_database_does_not_feed_itself_versions_forever() {
	// A sample commit is itself a version, so without the skip-if-unchanged guard each slice records the version
	// the previous slice created and an idle database allocates versions and epoch rows forever.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();
	commit_a_version(&engine, "ingest", 1);

	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	task.run_slice();
	let after_first = durable_samples(&engine);
	let version_after_first = engine.current_version().expect("head version");

	for _ in 0..5 {
		task.run_slice();
	}

	assert_eq!(
		durable_samples(&engine),
		after_first,
		"repeated slices on an unchanged database must not write another sample"
	);
	assert_eq!(
		engine.current_version().expect("head version"),
		version_after_first,
		"an idle database must not have its version advanced by the epoch log itself"
	);
}

#[test]
fn a_new_bucket_records_a_fresh_sample_once_the_version_moves() {
	// The liveness half of the idle guard: the guard must key on (bucket, version), not suppress sampling
	// outright, or coverage would stop advancing on a busy database too.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();

	commit_a_version(&engine, "ingest", 1);
	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	task.run_slice();

	clock.advance_minutes(5);
	commit_a_version(&engine, "ingest", 2);
	task.run_slice();

	let samples = durable_samples(&engine);
	assert_eq!(samples.len(), 2, "a new bucket with a moved version must add a sample; got {samples:?}");
	assert!(samples[1].0 >= samples[0].0 + MINUTE_SECS, "the second sample must land in a later bucket");
	assert!(samples[1].1 > samples[0].1, "the second sample must carry the newer version");
}

#[test]
fn pruning_drops_samples_beyond_the_horizon_and_keeps_the_ones_inside_it() {
	// The log is bounded by the longest declared ttl, not by a sample count; pruning inside that horizon
	// shortens the map's coverage below what the horizon promises.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();
	let horizon = engine.catalog().get_config_duration(ConfigKey::MaxRetentionHorizonFloor);
	assert_eq!(horizon, Duration::from_days(7).unwrap(), "precondition: the default horizon floor is seven days");

	commit_a_version(&engine, "ingest", 1);
	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	task.run_slice();
	let ancient = durable_samples(&engine)[0].0;

	clock.advance_days(8);
	commit_a_version(&engine, "ingest", 2);
	task.run_slice();

	let remaining = durable_samples(&engine);
	assert!(
		!remaining.iter().any(|(bucket, _, _)| *bucket == ancient),
		"a sample older than the horizon must be pruned; remaining {remaining:?}"
	);
	assert_eq!(remaining.len(), 1, "the sample inside the horizon must survive; remaining {remaining:?}");
}

#[test]
fn pruning_is_held_back_while_the_startup_gate_is_closed() {
	// Pruning is deletion like any other, so it waits out the startup grace - but sampling must continue during
	// it, or the gate creates the very coverage gap it is protecting against.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();

	commit_a_version(&engine, "ingest", 1);
	let gated = RetentionStartupGate::arm(engine.clock().clone(), Duration::from_days(30).unwrap());
	let mut task = EpochLogTask::new(engine.clone(), gated.clone());
	task.run_slice();
	let ancient = durable_samples(&engine)[0].0;

	clock.advance_days(8);
	commit_a_version(&engine, "ingest", 2);
	task.run_slice();

	let remaining = durable_samples(&engine);
	assert!(
		remaining.iter().any(|(bucket, _, _)| *bucket == ancient),
		"a closed gate must hold pruning back; remaining {remaining:?}"
	);
	assert_eq!(remaining.len(), 2, "sampling must continue while gated, only deletion waits; got {remaining:?}");
	assert!(gated.skipped_slices() > 0, "a gated prune must be counted, not silently skipped");
}

#[test]
fn a_sample_never_anchors_an_instant_to_version_zero() {
	// Anchoring an instant to version zero places "now" before every row in the database, so any reader of the
	// map would resolve a floor above live data.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();

	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	let progress = task.run_slice();

	assert_eq!(progress, Progress::Exhausted, "a slice with no pruning backlog must not ask for a catch-up");
	for (bucket, _at, version) in durable_samples(&engine) {
		assert!(version > 0, "bucket {bucket} was anchored to version zero, which precedes every live row");
	}
}

#[test]
fn hydrating_a_cold_database_leaves_the_epoch_resolving_nothing() {
	// A fresh database has no samples, so the map must answer none rather than a floor; "no coverage" can never
	// be allowed to read as "everything is below the floor".
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();

	let restored = hydrate(&engine, Duration::from_days(7).unwrap()).expect("hydration on a cold database");

	assert_eq!(restored, 0, "a cold database has nothing to hydrate");
	assert_eq!(
		VersionEpoch::new().floor_version_at(EpochSeconds::new(engine.clock().now().to_secs())),
		None,
		"a cold epoch must yield no cutoff, so gc deletes nothing"
	);
}

#[test]
fn the_class_is_paced_by_the_configured_bucket_width() {
	// The cadence has to follow the bucket: sampling faster than the bucket width writes nothing new (the guard
	// suppresses it), and slower leaves buckets with no sample, which is a resolution hole in the map.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();
	let task = EpochLogTask::new(engine.clone(), open_gate(&engine));

	assert_eq!(
		task.interval(),
		engine.catalog().get_config_duration(ConfigKey::EpochBucketInterval),
		"the epoch log must tick once per durable bucket"
	);
	assert_eq!(task.name(), "epoch-log", "the class name keys its metrics, report line and span");
}

#[test]
fn the_gate_is_the_only_thing_between_a_cold_start_and_the_whole_backlog() {
	// The factory relies on this when it arms the gate: an open gate reclaims immediately, otherwise the startup
	// grace is decorative.
	let t = TestEngine::new();
	let gate = RetentionStartupGate::arm(Clock::Mock(t.mock_clock()), Duration::from_seconds(0).unwrap());

	assert!(gate.is_open(), "a zero grace gate must not delay the first reclamation slice");
}
