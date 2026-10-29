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
		commit::flow_relevant,
	},
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

use crate::produce::watermark::CdcProducerWatermark;

pub struct CdcProducerActor<S> {
	storage: Arc<S>,
	event_bus: EventBus,

	watermark: CdcProducerWatermark,
}

impl<S> CdcProducerActor<S>
where
	S: CdcStorage + Send + Sync + 'static,
{
	pub fn new(storage: S, event_bus: EventBus, watermark: CdcProducerWatermark) -> Self {
		Self {
			storage: Arc::new(storage),
			event_bus,
			watermark,
		}
	}

	fn process(&self, version: ChangeVersion, changed_at: DateTime, deltas: Vec<Delta>) -> CdcStorageResult<()> {
		debug!(version = version.commit.0, delta_count = deltas.len(), "Processing CDC");

		let cdc_changes: Vec<CdcChange> =
			deltas.iter().filter(|delta| flow_relevant(delta)).map(delta_to_raw_cdc_change).collect();

		self.write_and_emit(version, changed_at, cdc_changes)
	}

	#[inline]
	fn write_and_emit(
		&self,
		version: ChangeVersion,
		changed_at: DateTime,
		cdc_changes: Vec<CdcChange>,
	) -> CdcStorageResult<()> {
		if cdc_changes.is_empty() {
			return Ok(());
		}
		let cdc = Arc::new(Cdc::new(version, changed_at, cdc_changes));
		self.storage.write(&cdc)?;
		debug!(version = version.commit.0, "CDC written successfully");
		self.emit_written_event(version.commit, &cdc.changes);
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
		while let Some(parked) = state.parked.remove(&next) {
			self.process(parked.version, parked.changed_at, parked.deltas)?;
			self.watermark.advance(CommitVersion(next));
			let Some(following) = next.checked_add(1) else {
				break;
			};
			next = following;
		}
		state.next = Some(next.max(floor));
		Ok(())
	}
}

#[inline]
fn delta_to_raw_cdc_change(delta: &Delta) -> CdcChange {
	match delta {
		Delta::Set {
			key,
			bytes,
			pre,
		} => {
			let encoded = key.encode();
			let pre = match pre {
				SetPre::Unknown => {
					panic!("a published set reached cdc with an unresolved pre: {:?}", key)
				}
				SetPre::Absent => None,
				SetPre::Present(bytes) => Some(bytes.clone()),
			};
			match pre {
				Some(pre) => CdcChange::Update {
					key: encoded,
					pre,
					post: bytes.clone(),
				},
				None => CdcChange::Insert {
					key: encoded,
					post: bytes.clone(),
				},
			}
		}
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Announced {
				pre,
			},
		} => CdcChange::Delete {
			key: key.encode(),
			pre: Some(pre.clone()),
			visible: true,
		},
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Unobserved {
				pre,
			},
		} => CdcChange::Delete {
			key: key.encode(),
			pre: Some(pre.clone()),
			visible: false,
		},
		Delta::Remove {
			key,
			announce: RemoveAnnounce::Silent,
		} => panic!("a silent remove the flow filter drops reached cdc mapping: {:?}", key),
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

impl<S> Actor for CdcProducerActor<S>
where
	S: CdcStorage + Send + Sync + 'static,
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

pub fn spawn_cdc_producer<S>(
	spawner: &ActorSpawner,
	storage: S,
	event_bus: EventBus,
	watermark: CdcProducerWatermark,
) -> CdcProduceHandle
where
	S: CdcStorage + Send + Sync + 'static,
{
	let actor = CdcProducerActor::new(storage, event_bus, watermark);
	spawner.spawn_coordination("cdc-producer", actor)
}

#[cfg(test)]
pub mod tests {
	#[cfg(reifydb_assertions)]
	use std::panic::{AssertUnwindSafe, catch_unwind};
	use std::thread::sleep;

	use reifydb_core::{
		interface::catalog::storage::StorageId,
		key::{ringbuffer::RingBufferMetadataKey, row::RowKey},
	};
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
	use reifydb_store_cdc::{config::CdcStoreConfig, store::CdcStore};
	use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

	use super::*;
	use crate::testing::{make_bytes, make_key};

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
	fn known_pre_images_render_as_inserts_and_updates() {
		// The tag alone must decide insert versus update; a wrong mapping hands consumers a new row where a row
		// changed.
		let deltas = vec![
			set("a", "new_a", SetPre::Absent),
			set("b", "new_b", SetPre::Present(make_bytes("old_b"))),
			set("c", "new_c", SetPre::Present(make_bytes("old_c"))),
			Delta::Set {
				key: RingBufferMetadataKey::new(StorageId::table(1)).into(),
				bytes: make_bytes("meta"),
				pre: SetPre::Absent,
			},
			set("d", "new_d", SetPre::Absent),
			Delta::remove_silent(make_key("e")),
		];
		let (actor, storage, _system) = actor_over();

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
	}

	fn actor_over() -> (CdcProducerActor<CdcStore>, CdcStore, ActorSystem) {
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let storage = CdcStore::new(CdcStoreConfig::memory(spawner.clone(), Clock::Real));
		let actor =
			CdcProducerActor::new(storage.clone(), EventBus::new(&spawner), CdcProducerWatermark::new());
		(actor, storage, actor_system)
	}

	#[test]
	fn an_unresolved_pre_that_reaches_the_producer_stops_it_instead_of_becoming_an_insert() {
		// Pre-images are resolved under the commit lock; an unknown one reaching cdc taken as "no previous
		// version" would publish an update as an insert, so the producer must stop and write nothing.
		let (actor, storage, _system) = actor_over();

		let produced = catch_unwind(AssertUnwindSafe(|| {
			actor.process(
				ChangeVersion::from(CommitVersion(3)),
				DateTime::from_nanos(1),
				vec![Delta::Set {
					key: RowKey::new(StorageId::table(1), RowNumber(1)).into(),
					bytes: make_bytes("row"),
					pre: SetPre::Unknown,
				}],
			)
		}));

		let message = produced.expect_err("an unresolved pre must stop the producer");
		let message = message.downcast_ref::<String>().cloned().unwrap_or_default();
		assert!(message.starts_with("a published set reached cdc with an unresolved pre"), "got {message}");
		assert!(storage.read(CommitVersion(3)).unwrap().is_none(), "nothing may be written for the version");
	}

	#[test]
	fn test_producer_processes_insert() {
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let storage = CdcStore::new(CdcStoreConfig::memory(spawner.clone(), Clock::Real));
		let event_bus = EventBus::new(&spawner);
		let handle = spawn_cdc_producer(&spawner, storage.clone(), event_bus, CdcProducerWatermark::new());

		let deltas = vec![Delta::Set {
			key: make_key("test_key"),
			bytes: make_bytes("test_value"),
			pre: SetPre::Absent,
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
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let storage = CdcStore::new(CdcStoreConfig::memory(spawner.clone(), Clock::Real));
		let event_bus = EventBus::new(&spawner);
		let handle = spawn_cdc_producer(&spawner, storage.clone(), event_bus, CdcProducerWatermark::new());

		let deltas = vec![
			Delta::Set {
				key: make_key("key1"),
				bytes: make_bytes("value1"),
				pre: SetPre::Absent,
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
}
