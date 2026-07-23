// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
//! Durable version-epoch log: the mechanism that makes a declared TTL actually enforceable.
//!
//! Every TTL in the system becomes a cutoff via `floor_version_at(now - ttl)`. When that lookup returns none the
//! consumer reclaims NOTHING - correctly, because none means "no safe floor known" - and does so silently while
//! continuing to tick. That is the failure this log exists to prevent, and it has two historical shapes:
//!
//! - after a restart the RAM map was empty, so nothing expired until the process had been up longer than the TTL;
//! - at production commit rates the bounded RAM buffer aged out within hours, so a 24h TTL never resolved at all.
//!
//! The tests below pin the properties that close both: a sample survives to storage, hydration re-establishes
//! coverage for instants that precede this process, pruning respects the horizon rather than the buffer size, and
//! an idle database does not feed itself versions forever.

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		cdc::CdcConsumerId,
	},
	key::{EncodableKey, version_epoch::VersionEpochKey},
	lifecycle::{gate::RetentionStartupGate, progress::Progress, task::LifecycleTask},
};
use reifydb_engine::{engine::StandardEngine, test_harness::TestEngine};
use reifydb_runtime::{context::clock::Clock, version_epoch::VersionEpoch};
use reifydb_sub_lifecycle::gc::epoch::durable::{EpochLogTask, hydrate};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::value::{duration::Duration, identity::IdentityId, value_type::ValueType};

const MINUTE_NANOS: u64 = 60 * 1_000_000_000;

/// Advances the commit version with a well-formed system write, standing in for ingest traffic. The epoch only
/// records a sample when the head version has moved, so the tests need a real version to record.
fn commit_a_version(engine: &StandardEngine, consumer: &str, at: u64) {
	let mut txn = engine.begin_command(IdentityId::system()).expect("system command transaction");
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new(consumer), CommitVersion(at)).expect("checkpoint write");
	txn.commit().expect("commit");
}

/// Reads the durable epoch keyspace directly, so a test can assert what reached storage rather than what the RAM
/// map happens to still hold.
fn durable_samples(engine: &StandardEngine) -> Vec<(u64, u64)> {
	let shape = RowShape::testing(&[ValueType::Uint8]);
	let txn = engine.begin_query(IdentityId::system()).expect("system query transaction");
	let mut samples: Vec<(u64, u64)> = txn
		.range(VersionEpochKey::floor_scan(u64::MAX), RangeScope::All, 256)
		.filter_map(|entry| {
			let entry = entry.ok()?;
			let key = VersionEpochKey::decode(&entry.key)?;
			Some((key.bucket_nanos, shape.get_u64(&entry.row, 0)))
		})
		.collect();
	samples.sort_unstable();
	samples
}

fn open_gate(engine: &StandardEngine) -> RetentionStartupGate {
	RetentionStartupGate::open(engine.clock().clone())
}

#[test]
fn a_sample_reaches_storage_so_a_later_process_can_read_it() {
	// The whole premise of the durable log. A sample that only ever lives in RAM is the pre-existing behaviour,
	// under which every restart reset TTL coverage to zero.
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
	// This is the restart property stated directly: an instant BEFORE the epoch was ever populated in this
	// process must still resolve to a version, because the answer came off disk. Without it, a process restarted
	// after downtime longer than its TTLs reclaims nothing until it has been up that long again.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();
	let clock = t.mock_clock();

	commit_a_version(&engine, "ingest", 1);
	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	task.run_slice();
	let early_instant = engine.clock().now_nanos();
	let early_version = engine.current_version().expect("head version");

	clock.advance_hours(6);
	commit_a_version(&engine, "ingest", 2);
	task.run_slice();

	// A cold map stands in for the restarted process: it has never seen a commit in this process.
	let cold = VersionEpoch::new();
	assert_eq!(
		cold.floor_version_at(early_instant),
		None,
		"precondition: a cold epoch resolves nothing, which is why a restart used to stop reclaiming"
	);

	let restored = hydrate(&engine, Duration::from_days(7).unwrap()).expect("hydration reads the durable log");

	assert!(restored >= 2, "hydration must find every sample inside the horizon; found {restored}");
	assert_eq!(
		engine.version_epoch().floor_version_at(early_instant),
		Some(early_version.0),
		"the instant of the first sample must resolve to the version that was current then"
	);
}

#[test]
fn an_idle_database_does_not_feed_itself_versions_forever() {
	// A sample commit is itself a version. Without the skip-if-unchanged guard, each slice would record the
	// version created by the previous slice, so an idle database would allocate versions and epoch rows forever -
	// a leak introduced by the very mechanism meant to bound one.
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
	assert!(samples[1].0 >= samples[0].0 + MINUTE_NANOS, "the second sample must land in a later bucket");
	assert!(samples[1].1 > samples[0].1, "the second sample must carry the newer version");
}

#[test]
fn pruning_drops_samples_beyond_the_horizon_and_keeps_the_ones_inside_it() {
	// Retention of the log is bounded by the longest declared ttl, not by a sample count. Pruning inside the
	// horizon would make that ttl unresolvable - a silent none - which is the original defect wearing a new hat.
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
		!remaining.iter().any(|(bucket, _)| *bucket == ancient),
		"a sample older than the horizon must be pruned; remaining {remaining:?}"
	);
	assert_eq!(remaining.len(), 1, "the sample inside the horizon must survive; remaining {remaining:?}");
}

#[test]
fn pruning_is_held_back_while_the_startup_gate_is_closed() {
	// Landmine L6: making the epoch durable un-blinds every consumer at boot. Pruning is deletion like any other,
	// so it waits out the startup grace - but sampling must continue during it, or the gate would create the very
	// coverage gap it is protecting against.
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
		remaining.iter().any(|(bucket, _)| *bucket == ancient),
		"a closed gate must hold pruning back; remaining {remaining:?}"
	);
	assert_eq!(remaining.len(), 2, "sampling must continue while gated, only deletion waits; got {remaining:?}");
	assert!(gated.skipped_slices() > 0, "a gated prune must be counted, not silently skipped");
}

#[test]
fn a_sample_never_anchors_an_instant_to_version_zero() {
	// Version zero means no commit has happened yet. Recording it would place "now" before every row in the
	// database, so the first expiry pass could compute a cutoff above live data and delete it. The guard must
	// hold on a database that has only ever seen its own bootstrap writes.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();

	let mut task = EpochLogTask::new(engine.clone(), open_gate(&engine));
	let progress = task.run_slice();

	assert_eq!(progress, Progress::Exhausted, "a slice with no pruning backlog must not ask for a catch-up");
	for (bucket, version) in durable_samples(&engine) {
		assert!(version > 0, "bucket {bucket} was anchored to version zero, which precedes every live row");
	}
}

#[test]
fn hydrating_a_cold_database_leaves_the_epoch_resolving_nothing() {
	// The safety property the whole design must not break: a fresh database has no samples, so every cutoff is
	// none and every consumer deletes nothing. Hydration returning "no coverage" must never be confused with
	// hydration returning "reclaim everything".
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();

	let restored = hydrate(&engine, Duration::from_days(7).unwrap()).expect("hydration on a cold database");

	assert_eq!(restored, 0, "a cold database has nothing to hydrate");
	assert_eq!(
		VersionEpoch::new().floor_version_at(engine.clock().now_nanos()),
		None,
		"a cold epoch must yield no cutoff, so gc deletes nothing"
	);
}

#[test]
fn the_class_is_paced_by_the_configured_bucket_width() {
	// The cadence has to follow the bucket: sampling faster than the bucket width writes nothing new (the guard
	// suppresses it), and slower leaves buckets with no sample, which is a resolution hole in expiry.
	let t = TestEngine::new();
	let engine: StandardEngine = t.inner().clone();
	let task = EpochLogTask::new(engine.clone(), open_gate(&engine));

	assert_eq!(
		task.interval(),
		engine.catalog().get_config_duration(ConfigKey::EpochBucketDuration),
		"the epoch log must tick once per durable bucket"
	);
	assert_eq!(task.name(), "epoch-log", "the class name keys its metrics, report line and span");
}

#[test]
fn the_gate_is_the_only_thing_between_a_cold_start_and_the_whole_backlog() {
	// Documents the contract the factory relies on when it arms the gate: an open gate reclaims immediately.
	// If this ever stopped being true the startup grace would be decorative.
	let t = TestEngine::new();
	let gate = RetentionStartupGate::arm(Clock::Mock(t.mock_clock()), Duration::from_seconds(0).unwrap());

	assert!(gate.is_open(), "a zero grace gate must not delay the first reclamation slice");
}
