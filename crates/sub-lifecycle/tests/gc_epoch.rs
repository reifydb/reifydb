// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Version-epoch backstop sampler.
//!
//! Version-anchored TTL asks "which commit version was current at wall-clock T?". The per-commit listener answers
//! that exactly, but it can lose events under mailbox pressure; this sampler is the backstop that keeps the map
//! advancing anyway. When the map goes cold, `floor_version_at` returns none, expiry computes an empty cutoff, and
//! reclamation deletes NOTHING - silently, with every task still ticking. That is the precise mechanism behind the
//! unbounded-growth incident this subsystem was built for, so the sampler's contract is load-bearing.

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::epoch::EpochSource,
};
use reifydb_runtime::{
	actor::{testing::TestHarness, traits::Directive},
	version_epoch::VersionEpoch,
};
use reifydb_sub_lifecycle::gc::epoch::actor::Actor;
use reifydb_value::value::{Value, duration::Duration};

/// A source whose clock and version the test drives explicitly, so sampling is deterministic.
struct ScriptedSource {
	now_nanos: Arc<AtomicU64>,
	/// `0` models "no commit has happened yet", which the sampler must treat as nothing-to-sample.
	version: Arc<AtomicU64>,
}

impl EpochSource for ScriptedSource {
	fn now_nanos(&self) -> u64 {
		self.now_nanos.load(Ordering::SeqCst)
	}

	fn current_version(&self) -> Option<CommitVersion> {
		match self.version.load(Ordering::SeqCst) {
			0 => None,
			v => Some(CommitVersion(v)),
		}
	}
}

struct StubConfig;

impl GetConfig for StubConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		match key {
			ConfigKey::VersionEpochSampleInterval => Value::Duration(Duration::from_seconds(1).unwrap()),
			other => panic!("the epoch sampler must not read config key {other}"),
		}
	}

	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		self.get_config(key)
	}
}

struct Fixture {
	epoch: VersionEpoch,
	now_nanos: Arc<AtomicU64>,
	version: Arc<AtomicU64>,
}

impl Fixture {
	fn build(now_nanos: u64, version: u64) -> (Self, TestHarness<Actor<ScriptedSource>>) {
		let epoch = VersionEpoch::new();
		let now = Arc::new(AtomicU64::new(now_nanos));
		let ver = Arc::new(AtomicU64::new(version));
		let actor = Actor::new(
			epoch.clone(),
			ScriptedSource {
				now_nanos: now.clone(),
				version: ver.clone(),
			},
			Arc::new(StubConfig),
		);
		// `init` performs the priming sample, so the harness models a real spawn.
		let harness = TestHarness::new(actor);
		(
			Fixture {
				epoch,
				now_nanos: now,
				version: ver,
			},
			harness,
		)
	}

	fn advance(&self, nanos: u64, version: u64) {
		self.now_nanos.store(nanos, Ordering::SeqCst);
		self.version.store(version, Ordering::SeqCst);
	}
}

const SECOND: u64 = 1_000_000_000;

#[test]
fn primes_the_map_on_startup_so_expiry_has_a_floor_before_the_first_tick() {
	// Without a priming sample the map is empty for a whole interval after boot. Every expiry check in that
	// window resolves to none and reclaims nothing, which on a short interval is a recurring stall.
	let (fixture, _harness) = Fixture::build(10 * SECOND, 42);

	assert_eq!(
		fixture.epoch.floor_version_at(10 * SECOND),
		Some(42),
		"the sampler must record a sample during init, not wait for its first tick"
	);
}

#[test]
fn a_tick_records_the_current_version_against_the_current_instant() {
	let (fixture, mut harness) = Fixture::build(10 * SECOND, 42);
	fixture.advance(20 * SECOND, 99);

	harness.send(reifydb_core::actors::version_epoch::VersionEpochMessage::Tick(
		reifydb_value::value::datetime::DateTime::from_nanos(20 * SECOND),
	));
	harness.process_all();

	assert_eq!(
		fixture.epoch.floor_version_at(20 * SECOND),
		Some(99),
		"a tick must record the version that is current at the sampled instant"
	);
	assert_eq!(
		fixture.epoch.floor_version_at(15 * SECOND),
		Some(42),
		"an earlier instant must still resolve to the version current then - the map is a history, not a cell"
	);
}

#[test]
fn records_nothing_before_the_first_commit_rather_than_anchoring_time_to_version_zero() {
	// Recording version 0 for "now" would make every row committed later look older than the floor, so the
	// first expiry pass after boot could delete live data. Absence is the safe answer here.
	let (fixture, _harness) = Fixture::build(10 * SECOND, 0);

	assert_eq!(
		fixture.epoch.floor_version_at(10 * SECOND),
		None,
		"with no committed version the sampler must record nothing at all"
	);
}

#[test]
fn a_stale_sample_never_lowers_a_floor_the_listener_already_set() {
	// The sampler is a BACKSTOP running alongside the per-commit listener. If a sample taken from a lagging
	// read could move a floor backwards, the backstop would resurrect already-reclaimable versions and stall
	// reclamation instead of advancing it.
	let (fixture, mut harness) = Fixture::build(10 * SECOND, 42);
	fixture.epoch.record(30 * SECOND, 500);

	fixture.advance(20 * SECOND, 99);
	harness.send(reifydb_core::actors::version_epoch::VersionEpochMessage::Tick(
		reifydb_value::value::datetime::DateTime::from_nanos(20 * SECOND),
	));
	harness.process_all();

	assert_eq!(
		fixture.epoch.floor_version_at(30 * SECOND),
		Some(500),
		"a backstop sample from behind the listener must not lower the established floor"
	);
}

#[test]
fn shutdown_stops_the_sampler() {
	let (_fixture, mut harness) = Fixture::build(10 * SECOND, 42);

	harness.send(reifydb_core::actors::version_epoch::VersionEpochMessage::Shutdown);
	let directives = harness.process_all();

	assert_eq!(directives, vec![Directive::Stop], "Shutdown must stop the sampler");
}

#[test]
fn a_cancelled_context_stops_the_sampler_without_recording() {
	let (fixture, mut harness) = Fixture::build(10 * SECOND, 42);
	fixture.advance(20 * SECOND, 99);

	harness.cancel();
	harness.send(reifydb_core::actors::version_epoch::VersionEpochMessage::Tick(
		reifydb_value::value::datetime::DateTime::from_nanos(20 * SECOND),
	));
	let directives = harness.process_all();

	assert_eq!(
		fixture.epoch.floor_version_at(20 * SECOND),
		Some(42),
		"a cancelled sampler must not record - 42 is the priming sample, unchanged"
	);
	assert_eq!(directives, vec![Directive::Stop], "a cancelled sampler must stop");
}
