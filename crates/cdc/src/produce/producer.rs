// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::{
		EventBus, EventListener,
		metric::{CdcEvictedEvent, CdcEviction, CdcWrite, CdcWrittenEvent},
		transaction::PostCommitEvent,
	},
	interface::{
		catalog::config::{ConfigKey, GetConfig},
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
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::{debug, error, trace};

use crate::{
	consume::{host::CdcHost, wake::CdcWakeRegistry},
	produce::watermark::CdcProducerWatermark,
	storage::CdcStorage,
};

const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

use reifydb_core::actors::cdc::{CdcProduceHandle, CdcProduceMessage};

pub struct CdcProducerActor<S, T, H> {
	storage: Arc<S>,
	transaction_store: Arc<T>,
	host: H,
	event_bus: EventBus,
	clock: Clock,

	watermark: CdcProducerWatermark,
	wake_registry: CdcWakeRegistry,
}

impl<S, T, H> CdcProducerActor<S, T, H>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
{
	pub fn new(
		storage: S,
		transaction_store: T,
		host: H,
		event_bus: EventBus,
		clock: Clock,
		watermark: CdcProducerWatermark,
		wake_registry: CdcWakeRegistry,
	) -> Self {
		Self {
			storage: Arc::new(storage),
			transaction_store: Arc::new(transaction_store),
			host,
			event_bus,
			clock,
			watermark,
			wake_registry,
		}
	}

	fn process(&self, version: CommitVersion, changed_at: DateTime, deltas: Vec<Delta>, flow_changes: Vec<Change>) {
		let mut system_changes: Vec<SystemChange> = Vec::new();

		trace!(version = version.0, delta_count = deltas.len(), "Processing CDC");

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
				value_bytes: s.value_bytes() as u64,
			})
			.collect();
		self.event_bus.emit(CdcWrittenEvent::new(entries, version));
	}

	fn try_cleanup(&self) {
		match self.find_eviction_target() {
			Ok(Some(cutoff_version)) => {
				if let Err(e) = self.evict_and_emit(cutoff_version) {
					error!("CDC cleanup failed: {:?}", e);
				}
			}
			Ok(None) => {}
			Err(e) => error!("CDC cleanup failed: {:?}", e),
		}
	}

	#[inline]
	fn find_eviction_target(&self) -> Result<Option<CommitVersion>> {
		let Some(ttl) = self.host.catalog().get_config_duration_opt(ConfigKey::CdcTtlDuration) else {
			return Ok(None);
		};
		let cutoff_nanos = self.clock.now_nanos().saturating_sub(ttl.as_nanos() as u64);
		let cutoff = DateTime::from_nanos(cutoff_nanos);
		let Some(cutoff_version) = self.storage.find_ttl_cutoff(cutoff)? else {
			return Ok(None);
		};
		if cutoff_version.0 == 0 {
			return Ok(None);
		}
		Ok(Some(cutoff_version))
	}

	#[inline]
	fn evict_and_emit(&self, cutoff_version: CommitVersion) -> Result<()> {
		let result = self.storage.drop_before(cutoff_version)?;
		if result.count == 0 {
			return Ok(());
		}
		debug!(cutoff = cutoff_version.0, deleted = result.count, "CDC TTL eviction completed");
		let drop_entries: Vec<CdcEviction> = result
			.entries
			.into_iter()
			.map(|e| CdcEviction {
				key: e.key,
				value_bytes: e.value_bytes,
			})
			.collect();
		self.event_bus.emit(CdcEvictedEvent::new(drop_entries, cutoff_version));
		Ok(())
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

	#[inline]
	fn on_tick(&self) {
		self.try_cleanup();
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

pub struct CdcProducerState {
	_timer_handle: Option<TimerHandle>,
}

impl<S, T, H> Actor for CdcProducerActor<S, T, H>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
{
	type State = CdcProducerState;
	type Message = CdcProduceMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		debug!("CDC producer actor started");
		let timer_handle = ctx.schedule_repeat(CLEANUP_INTERVAL, CdcProduceMessage::Tick);
		CdcProducerState {
			_timer_handle: Some(timer_handle),
		}
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		if ctx.is_cancelled() {
			debug!("CDC producer actor stopping");
			return Directive::Stop;
		}
		match msg {
			CdcProduceMessage::Produce {
				version,
				changed_at,
				deltas,
				flow_changes,
			} => self.on_produce(version, changed_at, deltas, flow_changes),
			CdcProduceMessage::Tick => self.on_tick(),
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("CDC producer actor stopped");
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

#[allow(clippy::too_many_arguments)]
pub fn spawn_cdc_producer<S, T, H>(
	spawner: &ActorSpawner,
	storage: S,
	transaction_store: T,
	host: H,
	event_bus: EventBus,
	clock: Clock,
	watermark: CdcProducerWatermark,
	wake_registry: CdcWakeRegistry,
) -> CdcProduceHandle
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
{
	let actor = CdcProducerActor::new(storage, transaction_store, host, event_bus, clock, watermark, wake_registry);
	spawner.spawn_system("cdc-producer", actor)
}

#[cfg(test)]
pub mod tests {
	use std::{thread::sleep, time::Duration};

	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
	use reifydb_store_multi::MultiStore;
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::{
		storage::memory::MemoryCdcStorage,
		testing::{TestCdcHost, make_key, make_row},
	};

	#[test]
	fn test_producer_processes_insert() {
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		let host = TestCdcHost::new();
		let clock = host.clock.clone();
		let handle = spawn_cdc_producer(
			&spawner,
			storage.clone(),
			resolver,
			host,
			event_bus,
			clock,
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
		sleep(Duration::from_millis(50));

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
		let host = TestCdcHost::new();
		let clock = host.clock.clone();
		let handle = spawn_cdc_producer(
			&spawner,
			storage.clone(),
			resolver,
			host,
			event_bus,
			clock,
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

		sleep(Duration::from_millis(50));

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		// Only the Set should produce CDC, not the Drop
		assert_eq!(cdc.system_changes.len(), 1);
	}
}
