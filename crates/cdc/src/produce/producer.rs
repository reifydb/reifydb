// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	actors::cdc::{CdcProduceHandle, CdcProduceMessage},
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
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};
use tracing::{debug, error, info};

use crate::{
	consume::{host::CdcHost, wake::CdcWakeRegistry, watermark::compute_watermark},
	produce::watermark::CdcProducerWatermark,
	storage::CdcStorage,
};

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
				value_bytes: s.value_bytes() as u64,
			})
			.collect();
		self.event_bus.emit(CdcWrittenEvent::new(entries, version));
	}

	fn try_cleanup(&self, state: &mut CdcProducerState) {
		let catalog = self.host.catalog();
		let batch_size = (catalog.get_config_uint8(ConfigKey::CdcTtlScanBatchSize) as usize).max(1);
		let max_batches = (catalog.get_config_uint8(ConfigKey::CdcTtlScanMaxBatchesPerTick) as usize).max(1);
		let evicted = match self.find_eviction_target() {
			Ok(Some(cutoff_version)) => {
				match self.evict_and_emit(cutoff_version, batch_size, max_batches) {
					Ok(evicted) => evicted,
					Err(e) => {
						error!(cutoff = cutoff_version.0, error = ?e, "CDC cleanup failed");
						0
					}
				}
			}
			Ok(None) => 0,
			Err(e) => {
				error!(error = ?e, "CDC cleanup failed");
				0
			}
		};
		if evicted > 0 {
			self.maybe_reclaim(state, catalog.get_config_duration(ConfigKey::CdcTtlReclaimInterval));
		}
	}

	fn maybe_reclaim(&self, state: &mut CdcProducerState, reclaim_interval: Duration) {
		let now = DateTime::from_nanos(self.clock.now_nanos());
		let due = state.last_reclaim.map(|last| now - last >= reclaim_interval).unwrap_or(true);
		if !due {
			return;
		}
		if let Err(e) = self.storage.vacuum() {
			error!(error = ?e, "CDC free-page reclaim failed");
		}
		state.last_reclaim = Some(now);
	}

	#[inline]
	fn find_eviction_target(&self) -> Result<Option<CommitVersion>> {
		let Some(ttl) = self.host.catalog().get_config_duration_opt(ConfigKey::CdcTtlDuration) else {
			return Ok(None);
		};
		let cutoff_nanos = self.clock.now_nanos().saturating_sub(ttl.to_std().as_nanos() as u64);
		let cutoff = DateTime::from_nanos(cutoff_nanos);
		let Some(ttl_cutoff) = self.storage.find_ttl_cutoff(cutoff)? else {
			return Ok(None);
		};
		let cutoff_version = match self.consumer_watermark()? {
			Some(watermark) => ttl_cutoff.min(CommitVersion(watermark.0.saturating_add(1))),
			None => ttl_cutoff,
		};
		if cutoff_version.0 == 0 {
			return Ok(None);
		}
		Ok(Some(cutoff_version))
	}

	#[inline]
	fn consumer_watermark(&self) -> Result<Option<CommitVersion>> {
		let mut query = self.host.begin_query()?;
		compute_watermark(&mut Transaction::Query(&mut query))
	}

	fn evict_and_emit(
		&self,
		cutoff_version: CommitVersion,
		batch_size: usize,
		max_batches: usize,
	) -> Result<usize> {
		let mut iterations = 0usize;
		let mut evicted = 0usize;
		loop {
			let result = self.storage.drop_before(cutoff_version, batch_size)?;
			if result.count > 0 {
				evicted += result.count;
				debug!(
					cutoff = cutoff_version.0,
					deleted = result.count,
					"CDC TTL eviction batch completed"
				);
				let drop_entries: Vec<CdcEviction> = result
					.entries
					.into_iter()
					.map(|e| CdcEviction {
						key: e.key,
						value_bytes: e.value_bytes,
					})
					.collect();
				self.event_bus.emit(CdcEvictedEvent::new(drop_entries, cutoff_version));
			}
			iterations += 1;
			if !result.more_remaining {
				break;
			}
			if iterations >= max_batches {
				debug!(
					cutoff = cutoff_version.0,
					iterations,
					"CDC TTL eviction hit per-tick budget with backlog remaining; continuing next tick"
				);
				break;
			}
		}
		Ok(evicted)
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
	fn on_tick(&self, state: &mut CdcProducerState) {
		self.try_cleanup(state);
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
	last_reclaim: Option<DateTime>,
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
		info!("CDC producer actor started");
		let interval = self.host.catalog().get_config_duration(ConfigKey::CdcTtlScanInterval);
		let timer_handle = ctx.schedule_repeat(interval, CdcProduceMessage::Tick);
		CdcProducerState {
			_timer_handle: Some(timer_handle),
			last_reclaim: None,
		}
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
				flow_changes,
			} => self.on_produce(version, changed_at, deltas, flow_changes),
			CdcProduceMessage::Tick => self.on_tick(state),
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
	use std::thread::sleep;

	use reifydb_core::interface::cdc::CdcConsumerId;
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
	use reifydb_store_multi::MultiStore;
	use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

	use super::*;
	use crate::{
		consume::checkpoint::CdcCheckpoint,
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
	fn eviction_never_passes_the_consumer_watermark() {
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(Pools::default(), Clock::Real);
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		let host = TestCdcHost::new();
		let clock = host.clock.clone();

		// Aggressive TTL: every already-written (old-timestamped) entry is TTL-eligible for eviction.
		host.catalog()
			.cache()
			.set_config(
				ConfigKey::CdcTtlDuration,
				CommitVersion(1),
				Value::Duration(Duration::from_milliseconds(1).unwrap()),
			)
			.unwrap();

		// CDC entries 1..=10, all timestamped far in the past relative to the host's mock clock.
		for v in 1..=10u64 {
			let cdc = Cdc::new(
				CommitVersion(v),
				DateTime::from_nanos(1000),
				vec![],
				vec![SystemChange::Insert {
					key: make_key(&format!("k{v}")),
					post: make_row("v"),
				}],
			);
			storage.write(&cdc).unwrap();
		}

		// All flows have durably processed only through version 4.
		let mut cmd = host.begin_command().unwrap();
		CdcCheckpoint::persist(&mut cmd, &CdcConsumerId::new("flow"), CommitVersion(4)).unwrap();
		cmd.commit().unwrap();

		let actor = CdcProducerActor::new(
			storage.clone(),
			resolver,
			host,
			event_bus,
			clock,
			CdcProducerWatermark::new(),
			CdcWakeRegistry::new(),
		);

		// TTL alone would evict everything (cutoff = 11). The consumer watermark caps the cutoff at
		// 5 (= 4 + 1), so versions 5..=10 - not yet processed by all flows - are never dropped.
		assert_eq!(
			actor.find_eviction_target().unwrap(),
			Some(CommitVersion(5)),
			"eviction cutoff must never pass the minimum consumer checkpoint + 1"
		);
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

		sleep(Duration::from_milliseconds(50).unwrap().to_std());

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		// Only the Set should produce CDC, not the Drop
		assert_eq!(cdc.system_changes.len(), 1);
	}
}
