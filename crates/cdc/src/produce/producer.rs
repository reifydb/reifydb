// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{
	actors::cdc::{CdcProduceHandle, CdcProduceMessage},
	common::CommitVersion,
	delta::{Delta, RemoveAnnounce},
	event::{
		EventBus, EventListener,
		metric::{CdcWrite, CdcWrittenEvent},
		transaction::PostCommitEvent,
	},
	interface::{
		cdc::{Cdc, CdcChange},
		store::MultiVersionGetPrevious,
	},
	key::{cdc_exclude::should_exclude_from_cdc, kind::KeyKind},
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
use reifydb_value::{byte_size::ByteSize, value::datetime::DateTime};
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

	fn process(&self, version: CommitVersion, changed_at: DateTime, deltas: Vec<Delta>) -> CdcStorageResult<()> {
		let mut cdc_changes: Vec<CdcChange> = Vec::new();

		debug!(version = version.0, delta_count = deltas.len(), "Processing CDC");

		for delta in deltas {
			if Self::is_excluded_kind(&delta) {
				continue;
			}
			if let Some(change) = self.delta_to_cdc_change(delta, version) {
				cdc_changes.push(change);
			}
		}

		self.write_and_emit(version, changed_at, cdc_changes)
	}

	#[inline]
	fn is_excluded_kind(delta: &Delta) -> bool {
		KeyKind::of(delta.key()).map(should_exclude_from_cdc).unwrap_or(false)
	}

	#[inline]
	fn delta_to_cdc_change(&self, delta: Delta, version: CommitVersion) -> Option<CdcChange> {
		delta_to_raw_cdc_change(&delta, self.transaction_store.as_ref(), version)
	}

	#[inline]
	fn write_and_emit(
		&self,
		version: CommitVersion,
		changed_at: DateTime,
		cdc_changes: Vec<CdcChange>,
	) -> CdcStorageResult<()> {
		if cdc_changes.is_empty() {
			self.backlog.publish(version, None);
			return Ok(());
		}
		let cdc = Arc::new(Cdc::new(version, changed_at, cdc_changes.clone()));
		self.storage.write(&cdc)?;
		debug!(version = version.0, "CDC written successfully");
		self.emit_written_event(version, &cdc_changes);
		let relevant = is_relevant_cdc(&cdc).then_some(cdc);
		self.backlog.publish(version, relevant);
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
		version: CommitVersion,
		changed_at: DateTime,
		deltas: Vec<Delta>,
	) -> CdcStorageResult<()> {
		state.parked.insert(
			version.0,
			Parked {
				changed_at,
				deltas,
			},
		);
		let floor = state.next.unwrap_or(version.0);
		let mut next = floor.min(version.0);
		let mut released = false;
		while let Some(parked) = state.parked.remove(&next) {
			self.process(CommitVersion(next), parked.changed_at, parked.deltas)?;
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
	transaction_store: &dyn MultiVersionGetPrevious,
	version: CommitVersion,
) -> Option<CdcChange> {
	match delta {
		Delta::Set {
			key,
			bytes,
		} => {
			let pre = transaction_store.get_previous_version(key, version).ok().flatten();
			Some(if let Some(prev) = pre {
				CdcChange::Update {
					key: key.clone(),
					pre: prev.bytes,
					post: bytes.clone(),
				}
			} else {
				CdcChange::Insert {
					key: key.clone(),
					post: bytes.clone(),
				}
			})
		}
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Announced {
				pre,
			},
		} => Some(CdcChange::Delete {
			key: key.clone(),
			pre: Some(pre.clone()),
			visible: true,
		}),
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Unobserved {
				pre,
			},
		} => Some(CdcChange::Delete {
			key: key.clone(),
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
					panic!("CDC producer failed to write version {}: {:?}", version.0, e);
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
	use std::{
		sync::atomic::{AtomicUsize, Ordering},
		thread::sleep,
		time::{Duration as StdDuration, Instant},
	};

	use reifydb_core::{interface::catalog::storage::StorageId, key::row::RowKey};
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
	use reifydb_store_cdc::{config::CdcStoreConfig, store::CdcStore};
	use reifydb_store_multi::MultiStore;
	use reifydb_value::{
		byte_size::ByteSize,
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
		}];

		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: CommitVersion(1),
				changed_at: DateTime::from_nanos(12345000),
				deltas,
			})
			.unwrap();

		sleep(Duration::from_milliseconds(50).unwrap().to_std());

		let cdc = storage.read(CommitVersion(1)).unwrap();
		assert!(cdc.is_some());
		let cdc = cdc.unwrap();
		assert_eq!(cdc.version, CommitVersion(1));
		assert_eq!(cdc.changes.len(), 1);

		match &cdc.changes[0] {
			CdcChange::Insert {
				key,
				post,
			} => {
				assert_eq!(key.as_ref(), b"test_key");
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
			},
			Delta::remove_silent(make_key("key2")),
		];

		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: CommitVersion(2),
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
				version: CommitVersion(1),
				changed_at: DateTime::from_nanos(1),
				deltas: vec![Delta::Set {
					key: RowKey::encoded(StorageId::table(1), RowNumber(1)),
					bytes: make_bytes("row"),
				}],
			})
			.unwrap();
		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: CommitVersion(2),
				changed_at: DateTime::from_nanos(2),
				deltas: vec![Delta::Set {
					key: make_key("unknown_kind_key"),
					bytes: make_bytes("value"),
				}],
			})
			.unwrap();

		let deadline = Instant::now() + StdDuration::from_secs(10);
		let items = loop {
			match backlog.pull(CommitVersion(0), CommitVersion(2), ByteSize::from_mib(1)) {
				BacklogPull::Hit {
					items,
					advance_to,
					..
				} if advance_to == CommitVersion(2) && !items.is_empty() => break items,
				_ => {
					assert!(Instant::now() < deadline, "producer never fed the backlog");
					sleep(StdDuration::from_millis(5));
				}
			}
		};
		assert_eq!(items.len(), 1, "only the flow-relevant commit may leave an entry");
		assert_eq!(items[0].version, CommitVersion(1));
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
