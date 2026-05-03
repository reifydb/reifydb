// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	sync::{Arc, Mutex},
	thread::sleep,
	time::{Duration, Instant},
};

use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_cdc::{
	produce::{producer::spawn_cdc_producer, watermark::CdcProducerWatermark},
	storage::{CdcStorage, memory::MemoryCdcStorage},
	testing::TestCdcHost,
};
use reifydb_core::{
	actors::cdc::{CdcProduceHandle, CdcProduceMessage},
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	event::{Event, EventBus, EventListener, metric::CdcEvictedEvent},
	interface::{
		catalog::config::ConfigKey,
		cdc::{Cdc, SystemChange},
	},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::{Clock, MockClock},
	pool::Pools,
};
use reifydb_store_multi::MultiStore;
use reifydb_type::{
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, duration::Duration as TypeDuration},
};

const POLL_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Slack window after sending a Tick within which we trust the actor has settled.
/// Used only by negative assertions ("nothing was evicted") - positive assertions
/// use the bounded-poll helpers instead.
const NEGATIVE_SETTLE: Duration = Duration::from_millis(150);

/// Bundles every handle a TTL test needs: actor handle, storage, mock clock, materialized
/// catalog (for setting `CDC_TTL_DURATION`), and event bus (for capturing eviction events).
struct TtlFixture {
	handle: CdcProduceHandle,
	storage: MemoryCdcStorage,
	mock: MockClock,
	catalog: MaterializedCatalog,
	event_bus: EventBus,
}

impl TtlFixture {
	/// Build a fresh fixture with the mock clock initialised to `initial_nanos`. The actor
	/// is spawned on its own `ActorSystem` so each test is fully isolated.
	fn new(initial_nanos: u64) -> Self {
		let storage = MemoryCdcStorage::new();
		let resolver = MultiStore::testing_memory();
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::with_clock(initial_nanos);
		let catalog = host.catalog.materialized().clone();
		let mock = host.mock.clone();
		let clock = host.clock.clone();

		let handle = spawn_cdc_producer(
			&actor_system,
			storage.clone(),
			resolver,
			host,
			event_bus.clone(),
			clock,
			CdcProducerWatermark::new(),
		);

		Self {
			handle,
			storage,
			mock,
			catalog,
			event_bus,
		}
	}

	/// Send a Tick message - this is what the periodic timer would normally trigger.
	fn tick(&self) {
		self.handle.actor_ref().send(CdcProduceMessage::Tick).expect("send Tick");
	}
}

fn set_ttl_secs(catalog: &MaterializedCatalog, secs: i64) {
	catalog.set_config(
		ConfigKey::CdcTtlDuration,
		CommitVersion(1),
		Value::Duration(TypeDuration::from_seconds(secs).unwrap()),
	)
	.expect("set CDC_TTL_DURATION");
}

fn write_cdc(storage: &MemoryCdcStorage, version: u64, timestamp_nanos: u64) {
	let cdc = Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(timestamp_nanos),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![version as u8]),
			post: EncodedRow(CowVec::new(vec![version as u8])),
		}],
	);
	storage.write(&cdc).expect("write CDC entry");
}

/// Bounded-poll until `check` is true. Panics on timeout. Use this for positive
/// assertions ("entry should disappear") so the tests stay green even under CI jitter.
fn await_until<F: Fn() -> bool>(label: &str, check: F) {
	let deadline = Instant::now() + POLL_TIMEOUT;
	while Instant::now() < deadline {
		if check() {
			return;
		}
		sleep(POLL_INTERVAL);
	}
	panic!("await_until({label}) timed out after {POLL_TIMEOUT:?}");
}

/// Wrapper that lets tests share an `Arc<L>` listener with the EventBus.
struct WrappedListener<L>(Arc<L>);
impl<E, L> EventListener<E> for WrappedListener<L>
where
	E: Event,
	L: EventListener<E>,
{
	fn on(&self, event: &E) {
		self.0.on(event);
	}
}

#[derive(Default)]
struct EvictionRecorder {
	events: Mutex<Vec<(CommitVersion, usize)>>,
}
impl EventListener<CdcEvictedEvent> for EvictionRecorder {
	fn on(&self, event: &CdcEvictedEvent) {
		self.events.lock().unwrap().push((*event.version(), event.entries().len()));
	}
}

#[test]
fn ttl_unset_does_not_evict_anything() {
	// Default config => CdcTtlDuration is the typed-null Value::None => the cleanup tick
	// must be a no-op even after the clock jumps forward by an hour.
	let f = TtlFixture::new(1_000_000_000);
	write_cdc(&f.storage, 1, 100);
	write_cdc(&f.storage, 2, 200);
	write_cdc(&f.storage, 3, 300);

	f.mock.advance_secs(3600);
	f.tick();
	sleep(NEGATIVE_SETTLE);

	assert_eq!(f.storage.min_version().unwrap(), Some(CommitVersion(1)));
	assert_eq!(f.storage.max_version().unwrap(), Some(CommitVersion(3)));
}

#[test]
fn ttl_evicts_all_when_every_entry_is_older_than_cutoff() {
	let f = TtlFixture::new(10_000_000_000); // now = 10 s
	set_ttl_secs(&f.catalog, 5);
	write_cdc(&f.storage, 1, 1_000_000_000); // t = 1 s
	write_cdc(&f.storage, 2, 2_000_000_000); // t = 2 s
	write_cdc(&f.storage, 3, 3_000_000_000); // t = 3 s

	// cutoff = now - 5 s = 5 s; every entry is below it.
	f.tick();

	await_until("storage drained", || f.storage.min_version().unwrap().is_none());
	assert_eq!(f.storage.max_version().unwrap(), None);
}

#[test]
fn ttl_keeps_all_when_every_entry_is_within_cutoff() {
	let f = TtlFixture::new(10_000_000_000); // now = 10 s
	set_ttl_secs(&f.catalog, 60);
	write_cdc(&f.storage, 1, 8_000_000_000); // t = 8 s
	write_cdc(&f.storage, 2, 9_000_000_000); // t = 9 s

	// cutoff = now - 60 s = -50 s (saturated to 0). All entries are >= 0 → kept.
	f.tick();
	sleep(NEGATIVE_SETTLE);

	assert_eq!(f.storage.min_version().unwrap(), Some(CommitVersion(1)));
	assert_eq!(f.storage.max_version().unwrap(), Some(CommitVersion(2)));
}

#[test]
fn ttl_partial_eviction_drops_only_old_entries() {
	let f = TtlFixture::new(20_000_000_000); // now = 20 s
	set_ttl_secs(&f.catalog, 10); // cutoff = 10 s
	write_cdc(&f.storage, 1, 5_000_000_000); // too old
	write_cdc(&f.storage, 2, 9_000_000_000); // too old
	write_cdc(&f.storage, 3, 11_000_000_000); // fresh
	write_cdc(&f.storage, 4, 15_000_000_000); // fresh

	f.tick();

	await_until("v2 dropped", || f.storage.read(CommitVersion(2)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(1)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(3)).unwrap().is_some());
	assert!(f.storage.read(CommitVersion(4)).unwrap().is_some());
	assert_eq!(f.storage.min_version().unwrap(), Some(CommitVersion(3)));
}

#[test]
fn ttl_boundary_entry_at_cutoff_is_kept() {
	// `find_ttl_cutoff` returns the smallest version with `timestamp >= cutoff`,
	// so an entry whose timestamp equals the cutoff is retained.
	let f = TtlFixture::new(20_000_000_000); // now = 20 s
	set_ttl_secs(&f.catalog, 10); // cutoff = 10 s
	write_cdc(&f.storage, 1, 9_999_999_999); // 1 ns before cutoff - drop
	write_cdc(&f.storage, 2, 10_000_000_000); // exactly at cutoff - keep
	write_cdc(&f.storage, 3, 10_000_000_001); // 1 ns after cutoff - keep

	f.tick();

	await_until("v1 dropped", || f.storage.read(CommitVersion(1)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(2)).unwrap().is_some());
	assert!(f.storage.read(CommitVersion(3)).unwrap().is_some());
}

#[test]
fn ttl_empty_storage_is_a_noop() {
	let f = TtlFixture::new(10_000_000_000);
	set_ttl_secs(&f.catalog, 5);

	f.tick();
	sleep(NEGATIVE_SETTLE); // give the actor time to confirm there is nothing to do.

	assert_eq!(f.storage.min_version().unwrap(), None);
}

#[test]
fn ttl_progressive_eviction_as_clock_advances() {
	// Entries become eligible for eviction one tick at a time as the mock clock advances.
	let f = TtlFixture::new(0);
	set_ttl_secs(&f.catalog, 10);
	write_cdc(&f.storage, 1, 0); // t = 0
	write_cdc(&f.storage, 2, 5_000_000_000); // t = 5 s
	write_cdc(&f.storage, 3, 10_000_000_000); // t = 10 s

	// Tick 1: now = 8 s, cutoff = -2 s (saturated to 0). Nothing < 0 → keep all.
	f.mock.advance_secs(8);
	f.tick();
	sleep(NEGATIVE_SETTLE);
	assert_eq!(f.storage.min_version().unwrap(), Some(CommitVersion(1)));

	// Tick 2: now = 12 s, cutoff = 2 s. Only v1 (t = 0) is older → drop v1.
	f.mock.advance_secs(4);
	f.tick();
	await_until("v1 dropped", || f.storage.read(CommitVersion(1)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(2)).unwrap().is_some());

	// Tick 3: now = 17 s, cutoff = 7 s. v2 (t = 5 s) becomes eligible → drop v2.
	f.mock.advance_secs(5);
	f.tick();
	await_until("v2 dropped", || f.storage.read(CommitVersion(2)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(3)).unwrap().is_some());

	// Tick 4: now = 25 s, cutoff = 15 s. v3 (t = 10 s) becomes eligible too → drop v3.
	f.mock.advance_secs(8);
	f.tick();
	await_until("storage drained", || f.storage.min_version().unwrap().is_none());
}

#[test]
fn ttl_emits_evicted_event_with_correct_cutoff() {
	// Evictions should produce a CdcEvictedEvent whose `version` is the first kept version
	// (i.e. the cutoff that was passed to `drop_before`) and whose `entries` lists the
	// dropped storage rows.
	let f = TtlFixture::new(20_000_000_000); // now = 20 s
	set_ttl_secs(&f.catalog, 10); // cutoff = 10 s
	write_cdc(&f.storage, 1, 5_000_000_000); // drop
	write_cdc(&f.storage, 2, 9_000_000_000); // drop
	write_cdc(&f.storage, 3, 11_000_000_000); // keep - first kept => cutoff_version
	write_cdc(&f.storage, 4, 15_000_000_000); // keep

	let recorder = Arc::new(EvictionRecorder::default());
	f.event_bus.register::<CdcEvictedEvent, _>(WrappedListener(recorder.clone()));

	f.tick();
	await_until("v1 dropped", || f.storage.read(CommitVersion(1)).unwrap().is_none());
	f.event_bus.wait_for_completion();

	let received = recorder.events.lock().unwrap().clone();
	assert_eq!(received.len(), 1, "expected exactly one CdcEvictedEvent");
	let (cutoff_version, dropped_count) = received[0];
	assert_eq!(cutoff_version, CommitVersion(3), "cutoff should be the first kept version");
	assert_eq!(dropped_count, 2);
}

#[test]
fn ttl_does_not_emit_event_when_nothing_is_evicted() {
	let f = TtlFixture::new(20_000_000_000);
	set_ttl_secs(&f.catalog, 60); // cutoff far in the past => no evictions
	write_cdc(&f.storage, 1, 18_000_000_000);
	write_cdc(&f.storage, 2, 19_000_000_000);

	let recorder = Arc::new(EvictionRecorder::default());
	f.event_bus.register::<CdcEvictedEvent, _>(WrappedListener(recorder.clone()));

	f.tick();
	sleep(NEGATIVE_SETTLE);
	f.event_bus.wait_for_completion();

	assert!(recorder.events.lock().unwrap().is_empty());
}

#[test]
fn ttl_setting_zero_duration_is_rejected_by_catalog() {
	// Sanity check that the validate hook is wired in - the catalog rejects zero TTLs at
	// the set_config boundary, so a misconfigured operator never reaches the producer.
	let catalog = MaterializedCatalog::new();
	let zero = Value::Duration(TypeDuration::from_seconds(0).unwrap());
	let err = catalog.set_config(ConfigKey::CdcTtlDuration, CommitVersion(1), zero).unwrap_err();
	assert_eq!(err.code, "CA_053");
}
