// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{
	actors::cdc::{CdcProduceHandle, CdcProduceMessage},
	common::{ChangeVersion, CommitVersion},
	delta::{Delta, RemoveAnnounce, SetPre},
	event::{
		EventBus, EventListener,
		metric::{CdcWrite, CdcWrittenEvent},
		transaction::PostCommitEvent,
	},
	interface::{
		cdc::{Cdc, CdcChange},
		store::{MultiVersionGetPrevious, MultiVersionRow},
	},
	key::{any::TaggedKey, cdc::should_exclude_from_cdc},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_store_cdc::storage::{CdcStorage, CdcStorageResult};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, value::datetime::DateTime};
use tracing::{debug, error, info};

use crate::{
	consume::{backlog::FlowBacklog, is_relevant_cdc, wake::CdcWakeRegistry},
	produce::watermark::CdcProducerWatermark,
};

pub struct CdcProducerActor<S, T> {
	storage: Arc<S>,
	transaction_store: Arc<T>,
	event_bus: EventBus,

	watermark: CdcProducerWatermark,
	wake_registry: CdcWakeRegistry,
	backlog: FlowBacklog,
}

impl<S, T> CdcProducerActor<S, T>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
{
	pub fn new(
		storage: S,
		transaction_store: T,
		event_bus: EventBus,
		watermark: CdcProducerWatermark,
		wake_registry: CdcWakeRegistry,
		backlog: FlowBacklog,
	) -> Self {
		Self {
			storage: Arc::new(storage),
			transaction_store: Arc::new(transaction_store),
			event_bus,
			watermark,
			wake_registry,
			backlog,
		}
	}

	fn process(&self, version: ChangeVersion, changed_at: DateTime, deltas: Vec<Delta>) -> CdcStorageResult<()> {
		debug!(version = version.commit.0, delta_count = deltas.len(), "Processing CDC");

		let deltas: Vec<Delta> = deltas.into_iter().filter(|delta| !Self::is_excluded_kind(delta)).collect();
		let unknown: Vec<TaggedKey> = deltas
			.iter()
			.filter_map(|delta| match delta {
				Delta::Set {
					key,
					pre: SetPre::Unknown,
					..
				} => Some(key.clone()),
				_ => None,
			})
			.collect();
		let mut previous = self.transaction_store.get_previous_many(&unknown, version.commit)?.into_iter();

		reifydb_assertions! {
			self.assert_known_pres(&deltas, version.commit);
		}

		let cdc_changes: Vec<CdcChange> =
			deltas.iter().filter_map(|delta| delta_to_raw_cdc_change(delta, &mut previous)).collect();

		self.write_and_emit(version, changed_at, cdc_changes)
	}

	#[inline]
	fn is_excluded_kind(delta: &Delta) -> bool {
		should_exclude_from_cdc(delta.key().kind())
	}

	#[cfg(reifydb_assertions)]
	fn assert_known_pres(&self, deltas: &[Delta], version: CommitVersion) {
		for delta in deltas {
			let Delta::Set {
				key,
				pre,
				..
			} = delta
			else {
				continue;
			};
			let expected = match pre {
				SetPre::Unknown => continue,
				SetPre::Absent => None,
				SetPre::Present(bytes) => Some(bytes),
			};
			let stored = self.transaction_store.get_previous_version(key, version).expect(
				"the previous version of a set with a known pre-image must be readable to check it",
			);
			assert_eq!(
				stored.as_ref().map(|row| &row.bytes),
				expected,
				"a set's known pre-image disagrees with the store before version {}: {:?}",
				version.0,
				key
			);
		}
	}

	#[inline]
	fn write_and_emit(
		&self,
		version: ChangeVersion,
		changed_at: DateTime,
		cdc_changes: Vec<CdcChange>,
	) -> CdcStorageResult<()> {
		if cdc_changes.is_empty() {
			self.backlog.publish(version.commit, None);
			return Ok(());
		}
		let cdc = Arc::new(Cdc::new(version, changed_at, cdc_changes.clone()));
		self.storage.write(&cdc)?;
		debug!(version = version.commit.0, "CDC written successfully");
		self.emit_written_event(version.commit, &cdc_changes);
		let relevant = is_relevant_cdc(&cdc).then_some(cdc);
		self.backlog.publish(version.commit, relevant);
		Ok(())
	}

	#[inline]
	fn emit_written_event(&self, version: CommitVersion, cdc_changes: &[CdcChange]) {
		let entries: Vec<CdcWrite> = cdc_changes
			.iter()
			.map(|s| CdcWrite {
				key: s.key().clone(),
				value_bytes: ByteSize::from_bytes(s.value_bytes() as u64),
			})
			.collect();
		self.event_bus.emit(CdcWrittenEvent::new(entries, version));
	}

	fn on_produce(
		&self,
		state: &mut CdcProducerState,
		version: ChangeVersion,
		changed_at: DateTime,
		deltas: Vec<Delta>,
	) -> CdcStorageResult<()> {
		state.parked.insert(
			version.commit.0,
			Parked {
				version,
				changed_at,
				deltas,
			},
		);
		let floor = state.next.unwrap_or(version.commit.0);
		let mut next = floor.min(version.commit.0);
		let mut released = false;
		while let Some(parked) = state.parked.remove(&next) {
			self.process(parked.version, parked.changed_at, parked.deltas)?;
			self.watermark.advance(CommitVersion(next));
			released = true;
			let Some(following) = next.checked_add(1) else {
				break;
			};
			next = following;
		}
		state.next = Some(next.max(floor));
		if released {
			self.wake_registry.notify_all();
			self.backlog.notify();
		}
		Ok(())
	}
}

#[inline]
fn delta_to_raw_cdc_change(
	delta: &Delta,
	previous: &mut impl Iterator<Item = Option<MultiVersionRow<TaggedKey>>>,
) -> Option<CdcChange> {
	match delta {
		Delta::Set {
			key,
			bytes,
			pre,
		} => {
			let encoded = key.encode();
			let pre = match pre {
				SetPre::Unknown => previous.next().flatten().map(|row| row.bytes),
				SetPre::Absent => None,
				SetPre::Present(bytes) => Some(bytes.clone()),
			};
			Some(match pre {
				Some(pre) => CdcChange::Update {
					key: encoded,
					pre,
					post: bytes.clone(),
				},
				None => CdcChange::Insert {
					key: encoded,
					post: bytes.clone(),
				},
			})
		}
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Announced {
				pre,
			},
		} => Some(CdcChange::Delete {
			key: key.encode(),
			pre: Some(pre.clone()),
			visible: true,
		}),
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Unobserved {
				pre,
			},
		} => Some(CdcChange::Delete {
			key: key.encode(),
			pre: Some(pre.clone()),
			visible: false,
		}),
		Delta::Remove {
			announce: RemoveAnnounce::Silent,
			..
		} => None,
	}
}

struct Parked {
	version: ChangeVersion,
	changed_at: DateTime,
	deltas: Vec<Delta>,
}

#[derive(Default)]
pub struct CdcProducerState {
	next: Option<u64>,
	parked: BTreeMap<u64, Parked>,
}

impl<S, T> Actor for CdcProducerActor<S, T>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
{
	type State = CdcProducerState;
	type Message = CdcProduceMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		info!("CDC producer actor started");
		CdcProducerState::default()
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		if ctx.is_cancelled() {
			info!("CDC producer actor stopping");
			return Directive::Stop;
		}
		match msg {
			CdcProduceMessage::Produce {
				version,
				changed_at,
				deltas,
			} => {
				if let Err(e) = self.on_produce(state, version, changed_at, deltas) {
					panic!("CDC producer failed to write version {}: {:?}", version.commit.0, e);
				}
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		info!("CDC producer actor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

pub struct CdcProducerEventListener {
	actor_ref: ActorRef<CdcProduceMessage>,
	clock: Clock,
}

impl CdcProducerEventListener {
	pub fn new(actor_ref: ActorRef<CdcProduceMessage>, clock: Clock) -> Self {
		Self {
			actor_ref,
			clock,
		}
	}
}

impl EventListener<PostCommitEvent> for CdcProducerEventListener {
	fn on(&self, event: &PostCommitEvent) {
		let msg = CdcProduceMessage::Produce {
			version: *event.version(),
			changed_at: self.clock.now(),
			deltas: event.deltas().iter().cloned().collect(),
		};

		if let Err(e) = self.actor_ref.send(msg) {
			error!("Failed to send CDC event to producer actor: {:?}", e);
		}
	}
}

pub fn spawn_cdc_producer<S, T>(
	spawner: &ActorSpawner,
	storage: S,
	transaction_store: T,
	event_bus: EventBus,
	watermark: CdcProducerWatermark,
	wake_registry: CdcWakeRegistry,
	backlog: FlowBacklog,
) -> CdcProduceHandle
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
{
	let actor = CdcProducerActor::new(storage, transaction_store, event_bus, watermark, wake_registry, backlog);
	spawner.spawn_coordination("cdc-producer", actor)
}

#[cfg(test)]
pub mod tests {
	#[cfg(reifydb_assertions)]
	use std::panic::{AssertUnwindSafe, catch_unwind};
	use std::{
		sync::atomic::{AtomicUsize, Ordering},
		thread::sleep,
	};

	use reifydb_core::{
		interface::{
			catalog::storage::StorageId,
			store::{MultiVersionCommit, MultiVersionRow},
		},
		key::{any::TaggedKey, ringbuffer::RingBufferMetadataKey, row::RowKey},
	};
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
	use reifydb_store_cdc::{config::CdcStoreConfig, error::CdcError, store::CdcStore};
	use reifydb_store_multi::MultiStore;
	use reifydb_value::{
		Result,
		byte_size::ByteSize,
		util::cowvec::CowVec,
		value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
	};

	use super::*;
	use crate::{
		consume::backlog::BacklogPull,
		testing::{make_bytes, make_key},
	};

	fn test_backlog() -> FlowBacklog {
		FlowBacklog::new(ByteSize::from_mib(16))
	}

	struct FailingStore;

	impl MultiVersionGetPrevious for FailingStore {
		fn get_previous_version(
			&self,
			_key: &TaggedKey,
			_before_version: CommitVersion,
		) -> Result<Option<MultiVersionRow<TaggedKey>>> {
			Err(CdcError::Internal("the previous version cannot be read".to_string()).into())
		}

		fn get_previous_many(
			&self,
			_keys: &[TaggedKey],
			_before_version: CommitVersion,
		) -> Result<Vec<Option<MultiVersionRow<TaggedKey>>>> {
			Err(CdcError::Internal("the previous versions cannot be read".to_string()).into())
		}
	}

	#[derive(Clone, Default)]
	struct Reads {
		point: Arc<AtomicUsize>,
		batches: Arc<AtomicUsize>,
		batched_keys: Arc<AtomicUsize>,
	}

	struct CountingStore {
		inner: MultiStore,
		reads: Reads,
	}

	impl MultiVersionGetPrevious for CountingStore {
		fn get_previous_version(
			&self,
			key: &TaggedKey,
			before_version: CommitVersion,
		) -> Result<Option<MultiVersionRow<TaggedKey>>> {
			self.reads.point.fetch_add(1, Ordering::SeqCst);
			self.inner.get_previous_version(key, before_version)
		}

		fn get_previous_many(
			&self,
			keys: &[TaggedKey],
			before_version: CommitVersion,
		) -> Result<Vec<Option<MultiVersionRow<TaggedKey>>>> {
			self.reads.batches.fetch_add(1, Ordering::SeqCst);
			self.reads.batched_keys.fetch_add(keys.len(), Ordering::SeqCst);
			self.inner.get_previous_many(keys, before_version)
		}
	}

	fn seeded() -> MultiStore {
		let store = MultiStore::testing_memory();
		MultiVersionCommit::commit(
			&store,
			CowVec::new(vec![
				Delta::Set {
					key: make_key("b"),
					bytes: make_bytes("old_b"),
					pre: SetPre::Unknown,
				},
				Delta::Set {
					key: make_key("c"),
					bytes: make_bytes("old_c"),
					pre: SetPre::Unknown,
				},
			]),
			CommitVersion(1),
		)
		.unwrap();
		store
	}

	fn set(key: &str, bytes: &str, pre: SetPre) -> Delta {
		Delta::Set {
			key: make_key(key),
			bytes: make_bytes(bytes),
			pre,
		}
	}

	fn rendered(cdc: &Cdc) -> Vec<String> {
		cdc.changes
			.iter()
			.map(|change| match change {
				CdcChange::Insert {
					key,
					post,
				} => format!("insert {key:?} {:?}", post.0.as_slice()),
				CdcChange::Update {
					key,
					pre,
					post,
				} => format!("update {key:?} {:?} -> {:?}", pre.0.as_slice(), post.0.as_slice()),
				CdcChange::Delete {
					key,
					..
				} => format!("delete {key:?}"),
			})
			.collect()
	}

	#[test]
	fn a_produce_reads_previous_versions_once_and_only_for_sets_whose_pre_image_is_unknown() {
		// The producer's read per set is the cost the pre-image tags remove; a set with a known pre-image
		// that is still read, or unknown sets read one by one, keep that cost, and a read answer handed to
		// the wrong set publishes another row's pre-image.
		let deltas = vec![
			set("a", "new_a", SetPre::Absent),
			set("b", "new_b", SetPre::Present(make_bytes("old_b"))),
			set("c", "new_c", SetPre::Unknown),
			Delta::Set {
				key: RingBufferMetadataKey::new(StorageId::table(1)).into(),
				bytes: make_bytes("meta"),
				pre: SetPre::Unknown,
			},
			set("d", "new_d", SetPre::Unknown),
			Delta::remove_silent(make_key("e")),
		];
		let inner = seeded();
		MultiVersionCommit::commit(&inner, CowVec::new(deltas.clone()), CommitVersion(2)).unwrap();
		let reads = Reads::default();
		let (actor, storage, _system) = actor_over(CountingStore {
			inner,
			reads: reads.clone(),
		});

		actor.process(ChangeVersion::from(CommitVersion(2)), DateTime::from_nanos(1), deltas).unwrap();

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		assert_eq!(
			rendered(&cdc),
			[
				format!("insert {:?} {:?}", make_key("a").encode(), b"new_a"),
				format!("update {:?} {:?} -> {:?}", make_key("b").encode(), b"old_b", b"new_b"),
				format!("update {:?} {:?} -> {:?}", make_key("c").encode(), b"old_c", b"new_c"),
				format!("insert {:?} {:?}", make_key("d").encode(), b"new_d"),
			]
		);
		assert_eq!(reads.batches.load(Ordering::SeqCst), 1, "one read for the whole commit");
		assert_eq!(reads.batched_keys.load(Ordering::SeqCst), 2, "only the unknown sets of included kinds");
		let checks = if cfg!(reifydb_assertions) {
			2
		} else {
			0
		};
		assert_eq!(reads.point.load(Ordering::SeqCst), checks, "point reads only check known pre-images");
	}

	#[cfg(reifydb_assertions)]
	#[test]
	fn a_known_pre_image_that_disagrees_with_the_store_is_caught() {
		// A wrong tag publishes an insert for a changed row or an update for a new one; every assertion
		// build must catch it at the producer instead of handing it to consumers.
		let (actor, _storage, _system) = actor_over(seeded());

		let produced = catch_unwind(AssertUnwindSafe(|| {
			actor.process(
				ChangeVersion::from(CommitVersion(2)),
				DateTime::from_nanos(1),
				vec![set("c", "new_c", SetPre::Absent)],
			)
		}));

		assert!(produced.is_err(), "a set tagged absent over a stored row must be caught");
	}

	fn actor_over<T: MultiVersionGetPrevious + Send + Sync + 'static>(
		store: T,
	) -> (CdcProducerActor<CdcStore, T>, CdcStore, ActorSystem) {
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let storage = CdcStore::new(CdcStoreConfig::memory(spawner.clone(), Clock::Real));
		let actor = CdcProducerActor::new(
			storage.clone(),
			store,
			EventBus::new(&spawner),
			CdcProducerWatermark::new(),
			CdcWakeRegistry::new(),
			test_backlog(),
		);
		(actor, storage, actor_system)
	}

	#[test]
	fn a_set_whose_previous_version_cannot_be_read_fails_the_produce_instead_of_becoming_an_insert() {
		// A read error that is taken for "no previous version" publishes an update as an insert, so every
		// consumer sees a new row where a row changed; the produce must fail instead.
		let (actor, storage, _system) = actor_over(FailingStore);

		let produced = actor.process(
			ChangeVersion::from(CommitVersion(3)),
			DateTime::from_nanos(1),
			vec![Delta::Set {
				key: RowKey::new(StorageId::table(1), RowNumber(1)).into(),
				bytes: make_bytes("row"),
				pre: SetPre::Unknown,
			}],
		);

		assert!(produced.is_err(), "the read error must reach the producer");
		assert!(storage.read(CommitVersion(3)).unwrap().is_none(), "nothing may be written for the version");
	}

	#[test]
	fn test_producer_processes_insert() {
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let storage = CdcStore::new(CdcStoreConfig::memory(spawner.clone(), Clock::Real));
		let event_bus = EventBus::new(&spawner);
		let handle = spawn_cdc_producer(
			&spawner,
			storage.clone(),
			resolver,
			event_bus,
			CdcProducerWatermark::new(),
			CdcWakeRegistry::new(),
			test_backlog(),
		);

		let deltas = vec![Delta::Set {
			key: make_key("test_key"),
			bytes: make_bytes("test_value"),
			pre: SetPre::Unknown,
		}];

		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: ChangeVersion::from(CommitVersion(1)),
				changed_at: DateTime::from_nanos(12345000),
				deltas,
			})
			.unwrap();

		sleep(Duration::from_milliseconds(50).unwrap().to_std());

		let cdc = storage.read(CommitVersion(1)).unwrap();
		assert!(cdc.is_some());
		let cdc = cdc.unwrap();
		assert_eq!(cdc.version.commit, CommitVersion(1));
		assert_eq!(cdc.changes.len(), 1);

		match &cdc.changes[0] {
			CdcChange::Insert {
				key,
				post,
			} => {
				assert_eq!(key, &make_key("test_key").encode());
				assert_eq!(post.0.as_slice(), b"test_value");
			}
			_ => panic!("Expected Insert change"),
		}
	}

	#[test]
	fn test_producer_skips_drop_operations() {
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let storage = CdcStore::new(CdcStoreConfig::memory(spawner.clone(), Clock::Real));
		let event_bus = EventBus::new(&spawner);
		let handle = spawn_cdc_producer(
			&spawner,
			storage.clone(),
			resolver,
			event_bus,
			CdcProducerWatermark::new(),
			CdcWakeRegistry::new(),
			test_backlog(),
		);

		let deltas = vec![
			Delta::Set {
				key: make_key("key1"),
				bytes: make_bytes("value1"),
				pre: SetPre::Unknown,
			},
			Delta::remove_silent(make_key("key2")),
		];

		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: ChangeVersion::from(CommitVersion(2)),
				changed_at: DateTime::from_nanos(12345000),
				deltas,
			})
			.unwrap();

		sleep(Duration::from_milliseconds(50).unwrap().to_std());

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		// A silent removal must not reach CDC; only the Set does.
		assert_eq!(cdc.changes.len(), 1);
	}

	#[test]
	fn produce_feeds_the_flow_backlog_and_wakes_it() {
		// The backlog is the flow hot path's only transport, so a produced commit must land there
		// and wake it without any consumer touching storage. A flow-irrelevant commit must still
		// extend coverage, or the next pull goes Behind for nothing.
		let store = MultiStore::testing_memory();
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let storage = CdcStore::new(CdcStoreConfig::memory(spawner.clone(), Clock::Real));
		let event_bus = EventBus::new(&spawner);
		let backlog = test_backlog();
		let woken = Arc::new(AtomicUsize::new(0));
		let counter = woken.clone();
		backlog.set_waker(move || {
			counter.fetch_add(1, Ordering::SeqCst);
		});
		let handle = spawn_cdc_producer(
			&spawner,
			storage.clone(),
			store,
			event_bus,
			CdcProducerWatermark::new(),
			CdcWakeRegistry::new(),
			backlog.clone(),
		);

		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: ChangeVersion::from(CommitVersion(1)),
				changed_at: DateTime::from_nanos(1),
				deltas: vec![Delta::Set {
					key: RowKey::new(StorageId::table(1), RowNumber(1)).into(),
					bytes: make_bytes("row"),
					pre: SetPre::Unknown,
				}],
			})
			.unwrap();
		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: ChangeVersion::from(CommitVersion(2)),
				changed_at: DateTime::from_nanos(2),
				deltas: vec![Delta::Set {
					key: make_key("unknown_kind_key"),
					bytes: make_bytes("value"),
					pre: SetPre::Unknown,
				}],
			})
			.unwrap();

		let deadline = Clock::Real.instant() + Duration::from_seconds(10).unwrap();
		let items = loop {
			match backlog.pull(CommitVersion(0), CommitVersion(2), ByteSize::from_mib(1)) {
				BacklogPull::Hit {
					items,
					advance_to,
					..
				} if advance_to == CommitVersion(2) && !items.is_empty() => break items,
				_ => {
					assert!(Clock::Real.instant() < deadline, "producer never fed the backlog");
					sleep(Duration::from_milliseconds(5).unwrap().to_std());
				}
			}
		};
		assert_eq!(items.len(), 1, "only the flow-relevant commit may leave an entry");
		assert_eq!(items[0].version.commit, CommitVersion(1));
		assert!(woken.load(Ordering::SeqCst) >= 1, "a produce must wake the backlog consumer");

		match backlog.pull(CommitVersion(1), CommitVersion(2), ByteSize::from_mib(1)) {
			BacklogPull::Hit {
				items,
				advance_to,
				..
			} => {
				assert!(items.is_empty(), "the unknown-kind commit must not leave an entry");
				assert_eq!(advance_to, CommitVersion(2), "but it must extend coverage");
			}
			BacklogPull::Behind => panic!("a produced version must be covered, not Behind"),
		}
	}
}
