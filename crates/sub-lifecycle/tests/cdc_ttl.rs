// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, Mutex};

use reifydb_catalog::cache::CatalogCache;
use reifydb_cdc::{
	storage::{CdcStorage, memory::MemoryCdcStorage},
	testing::TestCdcHost,
};
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	event::{Event, EventBus, EventListener, metric::CdcEvictedEvent},
	interface::{
		catalog::config::ConfigKey,
		cdc::{Cdc, SystemChange},
	},
	lifecycle::task::LifecycleTask,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::{Clock, MockClock},
	pool::Pools,
};
use reifydb_sub_lifecycle::cdc::ttl::CdcTtlTask;
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime},
};

/// Handles for a ttl test. `run_slice` runs on the test thread, so storage assertions need no polling; only the
/// async event bus is waited on.
struct TtlFixture {
	task: CdcTtlTask<MemoryCdcStorage, TestCdcHost>,
	storage: MemoryCdcStorage,
	mock: MockClock,
	catalog: CatalogCache,
	event_bus: EventBus,
	_actor_system: ActorSystem,
}

impl TtlFixture {
	/// Each fixture owns its `ActorSystem`, kept alive for the event bus, so tests stay isolated.
	fn new(initial_nanos: u64) -> Self {
		let storage = MemoryCdcStorage::new();
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		let host = TestCdcHost::with_clock(initial_nanos);
		let catalog = host.catalog.cache().clone();
		let mock = host.mock.clone();
		let clock = host.clock.clone();

		let task = CdcTtlTask::new(storage.clone(), host, event_bus.clone(), clock, None);

		Self {
			task,
			storage,
			mock,
			catalog,
			event_bus,
			_actor_system: actor_system,
		}
	}

	/// Drains to `Exhausted`, the way one periodic maintenance pass does.
	fn cleanup(&mut self) {
		while self.task.run_slice().is_yielded() {}
	}
}

fn set_ttl_secs(catalog: &CatalogCache, secs: i64) {
	catalog.set_config(ConfigKey::CdcTtlDuration, CommitVersion(1), Value::duration_seconds(secs))
		.expect("set CDC_TTL_DURATION");
}

fn write_cdc(storage: &MemoryCdcStorage, version: u64, timestamp_nanos: u64) {
	let cdc = Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(timestamp_nanos),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![version as u8]),
			post: EncodedBytes(CowVec::new(vec![version as u8])),
		}],
	);
	storage.write(&cdc).expect("write CDC entry");
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
		// Entries are per-source aggregates, so the number of CDC entries evicted is the sum of
		// their counts, not the number of entries.
		let total: u64 = event.entries().iter().map(|e| e.count.as_u64()).sum();
		self.events.lock().unwrap().push((*event.version(), total as usize));
	}
}

#[test]
fn ttl_unset_does_not_evict_anything() {
	// With no ttl configured CdcTtlDuration is none, so the cleanup pass must stay a no-op even after the
	// clock jumps forward by an hour.
	let mut f = TtlFixture::new(1_000_000_000);
	write_cdc(&f.storage, 1, 100);
	write_cdc(&f.storage, 2, 200);
	write_cdc(&f.storage, 3, 300);

	f.mock.advance_secs(3600);
	f.cleanup();

	assert_eq!(f.storage.min_version().unwrap(), Some(CommitVersion(1)));
	assert_eq!(f.storage.max_version().unwrap(), Some(CommitVersion(3)));
}

#[test]
fn ttl_evicts_all_when_every_entry_is_older_than_cutoff() {
	let mut f = TtlFixture::new(10_000_000_000); // now = 10 s
	set_ttl_secs(&f.catalog, 5);
	write_cdc(&f.storage, 1, 1_000_000_000); // t = 1 s
	write_cdc(&f.storage, 2, 2_000_000_000); // t = 2 s
	write_cdc(&f.storage, 3, 3_000_000_000); // t = 3 s

	// cutoff = now - 5 s = 5 s; every entry is below it.
	f.cleanup();

	assert_eq!(f.storage.min_version().unwrap(), None);
	assert_eq!(f.storage.max_version().unwrap(), None);
}

#[test]
fn ttl_keeps_all_when_every_entry_is_within_cutoff() {
	let mut f = TtlFixture::new(10_000_000_000); // now = 10 s
	set_ttl_secs(&f.catalog, 60);
	write_cdc(&f.storage, 1, 8_000_000_000); // t = 8 s
	write_cdc(&f.storage, 2, 9_000_000_000); // t = 9 s

	// cutoff = now - 60 s = -50 s (saturated to 0). All entries are >= 0 -> kept.
	f.cleanup();

	assert_eq!(f.storage.min_version().unwrap(), Some(CommitVersion(1)));
	assert_eq!(f.storage.max_version().unwrap(), Some(CommitVersion(2)));
}

#[test]
fn ttl_partial_eviction_drops_only_old_entries() {
	let mut f = TtlFixture::new(20_000_000_000); // now = 20 s
	set_ttl_secs(&f.catalog, 10); // cutoff = 10 s
	write_cdc(&f.storage, 1, 5_000_000_000); // too old
	write_cdc(&f.storage, 2, 9_000_000_000); // too old
	write_cdc(&f.storage, 3, 11_000_000_000); // fresh
	write_cdc(&f.storage, 4, 15_000_000_000); // fresh

	f.cleanup();

	assert!(f.storage.read(CommitVersion(1)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(2)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(3)).unwrap().is_some());
	assert!(f.storage.read(CommitVersion(4)).unwrap().is_some());
	assert_eq!(f.storage.min_version().unwrap(), Some(CommitVersion(3)));
}

#[test]
fn ttl_boundary_entry_at_cutoff_is_kept() {
	// `find_ttl_cutoff` returns the smallest version with `timestamp >= cutoff`,
	// so an entry whose timestamp equals the cutoff is retained.
	let mut f = TtlFixture::new(20_000_000_000); // now = 20 s
	set_ttl_secs(&f.catalog, 10); // cutoff = 10 s
	write_cdc(&f.storage, 1, 9_999_999_999); // 1 ns before cutoff - drop
	write_cdc(&f.storage, 2, 10_000_000_000); // exactly at cutoff - keep
	write_cdc(&f.storage, 3, 10_000_000_001); // 1 ns after cutoff - keep

	f.cleanup();

	assert!(f.storage.read(CommitVersion(1)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(2)).unwrap().is_some());
	assert!(f.storage.read(CommitVersion(3)).unwrap().is_some());
}

#[test]
fn ttl_empty_storage_is_a_noop() {
	let mut f = TtlFixture::new(10_000_000_000);
	set_ttl_secs(&f.catalog, 5);

	f.cleanup();

	assert_eq!(f.storage.min_version().unwrap(), None);
}

#[test]
fn ttl_progressive_eviction_as_clock_advances() {
	// Entries become eligible for eviction one cleanup pass at a time as the mock clock advances.
	let mut f = TtlFixture::new(0);
	set_ttl_secs(&f.catalog, 10);
	write_cdc(&f.storage, 1, 0); // t = 0
	write_cdc(&f.storage, 2, 5_000_000_000); // t = 5 s
	write_cdc(&f.storage, 3, 10_000_000_000); // t = 10 s

	// Pass 1: now = 8 s, cutoff = -2 s (saturated to 0). Nothing < 0 -> keep all.
	f.mock.advance_secs(8);
	f.cleanup();
	assert_eq!(f.storage.min_version().unwrap(), Some(CommitVersion(1)));

	// Pass 2: now = 12 s, cutoff = 2 s. Only v1 (t = 0) is older -> drop v1.
	f.mock.advance_secs(4);
	f.cleanup();
	assert!(f.storage.read(CommitVersion(1)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(2)).unwrap().is_some());

	// Pass 3: now = 17 s, cutoff = 7 s. v2 (t = 5 s) becomes eligible -> drop v2.
	f.mock.advance_secs(5);
	f.cleanup();
	assert!(f.storage.read(CommitVersion(2)).unwrap().is_none());
	assert!(f.storage.read(CommitVersion(3)).unwrap().is_some());

	// Pass 4: now = 25 s, cutoff = 15 s. v3 (t = 10 s) becomes eligible too -> drop v3.
	f.mock.advance_secs(8);
	f.cleanup();
	assert_eq!(f.storage.min_version().unwrap(), None);
}

#[test]
fn ttl_emits_evicted_event_with_correct_cutoff() {
	// The event's `version` is the first KEPT version - the cutoff handed to `drop_before` - and `entries`
	// lists the dropped storage rows.
	let mut f = TtlFixture::new(20_000_000_000); // now = 20 s
	set_ttl_secs(&f.catalog, 10); // cutoff = 10 s
	write_cdc(&f.storage, 1, 5_000_000_000); // drop
	write_cdc(&f.storage, 2, 9_000_000_000); // drop
	write_cdc(&f.storage, 3, 11_000_000_000); // keep - first kept => cutoff_version
	write_cdc(&f.storage, 4, 15_000_000_000); // keep

	let recorder = Arc::new(EvictionRecorder::default());
	f.event_bus.register::<CdcEvictedEvent, _>(WrappedListener(recorder.clone()));

	f.cleanup();
	f.event_bus.wait_for_completion();

	let received = recorder.events.lock().unwrap().clone();
	assert_eq!(received.len(), 1, "expected exactly one CdcEvictedEvent");
	let (cutoff_version, dropped_count) = received[0];
	assert_eq!(cutoff_version, CommitVersion(3), "cutoff should be the first kept version");
	assert_eq!(dropped_count, 2);
}

#[test]
fn ttl_does_not_emit_event_when_nothing_is_evicted() {
	let mut f = TtlFixture::new(20_000_000_000);
	set_ttl_secs(&f.catalog, 60); // cutoff far in the past => no evictions
	write_cdc(&f.storage, 1, 18_000_000_000);
	write_cdc(&f.storage, 2, 19_000_000_000);

	let recorder = Arc::new(EvictionRecorder::default());
	f.event_bus.register::<CdcEvictedEvent, _>(WrappedListener(recorder.clone()));

	f.cleanup();
	f.event_bus.wait_for_completion();

	assert!(recorder.events.lock().unwrap().is_empty());
}

#[test]
fn ttl_setting_zero_duration_is_rejected_by_catalog() {
	// The catalog rejects a zero ttl at the set_config boundary, so a misconfigured operator never reaches
	// the task.
	let catalog = CatalogCache::new();
	let zero = Value::duration_seconds(0);
	let err = catalog.set_config(ConfigKey::CdcTtlDuration, CommitVersion(1), zero).unwrap_err();
	assert_eq!(err.code, "CA_053");
}
