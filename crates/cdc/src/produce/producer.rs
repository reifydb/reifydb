// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

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
		cdc::{Cdc, SystemChange},
		store::MultiVersionGetPrevious,
	},
	key::{Key, cdc_exclude::should_exclude_from_cdc},
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
use reifydb_value::{byte_size::ByteSize, value::datetime::DateTime};
use tracing::{debug, error, info};

use crate::{
	consume::{backlog::FlowBacklog, is_relevant_cdc, wake::CdcWakeRegistry},
	produce::watermark::CdcProducerWatermark,
	storage::CdcStorage,
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

	fn process(&self, version: CommitVersion, changed_at: DateTime, deltas: Vec<Delta>) {
		let mut system_changes: Vec<SystemChange> = Vec::new();

		debug!(version = version.0, delta_count = deltas.len(), "Processing CDC");

		for delta in deltas {
			if Self::is_excluded_kind(&delta) {
				continue;
			}
			if let Some(change) = self.delta_to_system_change(delta, version) {
				system_changes.push(change);
			}
		}

		self.write_and_emit(version, changed_at, system_changes);
	}

	#[inline]
	fn is_excluded_kind(delta: &Delta) -> bool {
		Key::kind(delta.key()).map(should_exclude_from_cdc).unwrap_or(false)
	}

	#[inline]
	fn delta_to_system_change(&self, delta: Delta, version: CommitVersion) -> Option<SystemChange> {
		delta_to_raw_system_change(&delta, self.transaction_store.as_ref(), version)
	}

	#[inline]
	fn write_and_emit(&self, version: CommitVersion, changed_at: DateTime, system_changes: Vec<SystemChange>) {
		if system_changes.is_empty() {
			self.backlog.publish(version, None);
			return;
		}
		let cdc = Arc::new(Cdc::new(version, changed_at, system_changes.clone()));
		match self.storage.write(&cdc) {
			Ok(_) => {
				debug!(version = version.0, "CDC written successfully");
				self.emit_written_event(version, &system_changes);
			}
			Err(e) => error!(version = version.0, "CDC write failed: {:?}", e),
		}
		let relevant = is_relevant_cdc(&cdc).then_some(cdc);
		self.backlog.publish(version, relevant);
	}

	#[inline]
	fn emit_written_event(&self, version: CommitVersion, system_changes: &[SystemChange]) {
		let entries: Vec<CdcWrite> = system_changes
			.iter()
			.map(|s| CdcWrite {
				key: s.key().clone(),
				value_bytes: ByteSize::from_bytes(s.value_bytes() as u64),
			})
			.collect();
		self.event_bus.emit(CdcWrittenEvent::new(entries, version));
	}

	#[inline]
	fn on_produce(&self, version: CommitVersion, changed_at: DateTime, deltas: Vec<Delta>) {
		self.process(version, changed_at, deltas);

		self.watermark.advance(version);
		self.wake_registry.notify_all();
		self.backlog.notify();
	}
}

#[inline]
fn delta_to_raw_system_change(
	delta: &Delta,
	transaction_store: &dyn MultiVersionGetPrevious,
	version: CommitVersion,
) -> Option<SystemChange> {
	match delta {
		Delta::Set {
			key,
			bytes,
		} => {
			let pre = transaction_store.get_previous_version(key, version).ok().flatten();
			Some(if let Some(prev) = pre {
				SystemChange::Update {
					key: key.clone(),
					pre: prev.bytes,
					post: bytes.clone(),
				}
			} else {
				SystemChange::Insert {
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
		} => Some(SystemChange::Delete {
			key: key.clone(),
			pre: Some(pre.clone()),
			visible: true,
		}),
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Unobserved {
				pre,
			},
		} => Some(SystemChange::Delete {
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

pub struct CdcProducerState;

impl<S, T> Actor for CdcProducerActor<S, T>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
{
	type State = CdcProducerState;
	type Message = CdcProduceMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		info!("CDC producer actor started");
		CdcProducerState
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		if ctx.is_cancelled() {
			info!("CDC producer actor stopping");
			return Directive::Stop;
		}
		match msg {
			CdcProduceMessage::Produce {
				version,
				changed_at,
				deltas,
			} => self.on_produce(version, changed_at, deltas),
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
	use reifydb_store_multi::MultiStore;
	use reifydb_value::{
		byte_size::ByteSize,
		value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
	};

	use super::*;
	use crate::{
		consume::backlog::BacklogPull,
		storage::memory::MemoryCdcStorage,
		testing::{make_bytes, make_key},
	};

	fn test_backlog() -> FlowBacklog {
		FlowBacklog::new(ByteSize::from_mib(16))
	}

	#[test]
	fn test_producer_processes_insert() {
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
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
		assert_eq!(cdc.system_changes.len(), 1);

		match &cdc.system_changes[0] {
			SystemChange::Insert {
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
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
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
		assert_eq!(cdc.system_changes.len(), 1);
	}

	#[test]
	fn produce_feeds_the_flow_backlog_and_wakes_it() {
		// The backlog is the flow hot path's only transport, so a produced commit must land there
		// and wake it without any consumer touching storage. A flow-irrelevant commit must still
		// extend coverage, or the next pull goes Behind for nothing.
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
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
