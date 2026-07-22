// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::cdc::{CdcProduceHandle, CdcProduceMessage},
	common::CommitVersion,
	delta::Delta,
	event::{
		EventBus, EventListener,
		metric::{CdcWrite, CdcWrittenEvent},
		transaction::PostCommitEvent,
	},
	interface::{
		cdc::{Cdc, SystemChange},
		change::Change,
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

use crate::{consume::wake::CdcWakeRegistry, produce::watermark::CdcProducerWatermark, storage::CdcStorage};

pub struct CdcProducerActor<S, T> {
	storage: Arc<S>,
	transaction_store: Arc<T>,
	event_bus: EventBus,

	watermark: CdcProducerWatermark,
	wake_registry: CdcWakeRegistry,
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
	) -> Self {
		Self {
			storage: Arc::new(storage),
			transaction_store: Arc::new(transaction_store),
			event_bus,
			watermark,
			wake_registry,
		}
	}

	fn process(&self, version: CommitVersion, changed_at: DateTime, deltas: Vec<Delta>, flow_changes: Vec<Change>) {
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

		let changes: Vec<Change> = flow_changes
			.into_iter()
			.map(|mut change| {
				change.version = version;
				change.changed_at = changed_at;
				change
			})
			.collect();
		self.write_and_emit(version, changed_at, changes, system_changes);
	}

	#[inline]
	fn is_excluded_kind(delta: &Delta) -> bool {
		Key::kind(delta.key()).map(should_exclude_from_cdc).unwrap_or(false)
	}

	#[inline]
	fn delta_to_system_change(&self, delta: Delta, version: CommitVersion) -> Option<SystemChange> {
		match &delta {
			Delta::Set {
				..
			}
			| Delta::Unset {
				..
			} => delta_to_raw_system_change(&delta, self.transaction_store.as_ref(), version),
			Delta::Remove {
				..
			} => {
				let Delta::Remove {
					key,
				} = delta
				else {
					unreachable!()
				};
				Some(SystemChange::Delete {
					key,
					pre: None,
				})
			}
			Delta::Drop {
				..
			} => None,
		}
	}

	#[inline]
	fn write_and_emit(
		&self,
		version: CommitVersion,
		changed_at: DateTime,
		changes: Vec<Change>,
		system_changes: Vec<SystemChange>,
	) {
		if changes.is_empty() && system_changes.is_empty() {
			return;
		}
		let cdc = Cdc::new(version, changed_at, changes, system_changes.clone());
		match self.storage.write(&cdc) {
			Ok(_) => {
				debug!(version = version.0, "CDC written successfully");
				self.emit_written_event(version, &system_changes);
			}
			Err(e) => error!(version = version.0, "CDC write failed: {:?}", e),
		}
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
	fn on_produce(
		&self,
		version: CommitVersion,
		changed_at: DateTime,
		deltas: Vec<Delta>,
		flow_changes: Vec<Change>,
	) {
		self.process(version, changed_at, deltas, flow_changes);

		self.watermark.advance(version);
		self.wake_registry.notify_all();
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
			row,
		} => {
			let pre = transaction_store.get_previous_version(key, version).ok().flatten();
			Some(if let Some(prev) = pre {
				SystemChange::Update {
					key: key.clone(),
					pre: prev.row,
					post: row.clone(),
				}
			} else {
				SystemChange::Insert {
					key: key.clone(),
					post: row.clone(),
				}
			})
		}
		Delta::Unset {
			key,
			row,
		} => Some(SystemChange::Delete {
			key: key.clone(),
			pre: if row.is_empty() {
				None
			} else {
				Some(row.clone())
			},
		}),
		_ => None,
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
				flow_changes,
			} => self.on_produce(version, changed_at, deltas, flow_changes),
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
			changed_at: DateTime::from_nanos(self.clock.now_nanos()),
			deltas: event.deltas().iter().cloned().collect(),
			flow_changes: event.flow_changes().clone(),
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
) -> CdcProduceHandle
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
{
	let actor = CdcProducerActor::new(storage, transaction_store, event_bus, watermark, wake_registry);
	spawner.spawn_coordination("cdc-producer", actor)
}

#[cfg(test)]
pub mod tests {
	use std::thread::sleep;

	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
	use reifydb_store_multi::MultiStore;
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::*;
	use crate::{
		storage::memory::MemoryCdcStorage,
		testing::{make_key, make_row},
	};

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
		);

		let deltas = vec![Delta::Set {
			key: make_key("test_key"),
			row: make_row("test_value"),
		}];

		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: CommitVersion(1),
				changed_at: DateTime::from_nanos(12345000),
				deltas,
				flow_changes: vec![],
			})
			.unwrap();

		// Give actor time to process
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
		);

		let deltas = vec![
			Delta::Set {
				key: make_key("key1"),
				row: make_row("value1"),
			},
			Delta::Drop {
				key: make_key("key2"),
			},
		];

		handle.actor_ref()
			.send(CdcProduceMessage::Produce {
				version: CommitVersion(2),
				changed_at: DateTime::from_nanos(12345000),
				deltas,
				flow_changes: vec![],
			})
			.unwrap();

		sleep(Duration::from_milliseconds(50).unwrap().to_std());

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		// Only the Set should produce CDC, not the Drop
		assert_eq!(cdc.system_changes.len(), 1);
	}
}
