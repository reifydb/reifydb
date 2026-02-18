// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::{
		EventBus, EventListener,
		metric::{CdcEntryDrop, CdcEntryStats, CdcStatsDroppedEvent, CdcStatsRecordedEvent},
		transaction::PostCommitEvent,
	},
	interface::{
		catalog::primitive::PrimitiveId,
		cdc::{Cdc, SystemChange},
		change::{Change, Diff},
		store::MultiVersionGetPrevious,
	},
	key::{EncodableKey, Key, cdc_exclude::should_exclude_from_cdc, kind::KeyKind, row::RowKey},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorHandle, ActorSystem},
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	clock::Clock,
};
use reifydb_transaction::transaction::Transaction;
use tracing::{debug, error, trace};

use crate::{
	consume::{host::CdcHost, watermark::compute_watermark},
	storage::CdcStorage,
};

/// Default interval between CDC cleanup attempts (30 seconds)
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Message type for the CDC producer actor.
#[derive(Clone, Debug)]
pub enum CdcProduceMsg {
	Produce {
		version: CommitVersion,
		timestamp: u64,
		deltas: Vec<Delta>,
	},
	Tick,
}

/// Actor that processes CDC work items.
///
/// Receives commit data and generates CDC entries, writing them to storage.
/// Uses the shared ActorRuntime, so it works in both native and WASM.
/// Also performs periodic cleanup of old CDC entries based on consumer watermarks.
pub struct CdcProducerActor<S, T, H> {
	storage: Arc<S>,
	transaction_store: Arc<T>,
	host: H,
	event_bus: EventBus,
}

impl<S, T, H> CdcProducerActor<S, T, H>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
{
	pub fn new(storage: S, transaction_store: T, host: H, event_bus: EventBus) -> Self {
		Self {
			storage: Arc::new(storage),
			transaction_store: Arc::new(transaction_store),
			host,
			event_bus,
		}
	}

	fn process(&self, version: CommitVersion, timestamp: u64, deltas: Vec<Delta>) {
		let mut diffs_by_primitive: HashMap<PrimitiveId, Vec<Diff>> = HashMap::new();
		let mut system_changes: Vec<SystemChange> = Vec::new();
		let registry = self.host.schema_registry();

		trace!(version = version.0, delta_count = deltas.len(), "Processing CDC");

		for delta in deltas {
			let key = delta.key().clone();

			// Skip internal system keys that shouldn't appear in CDC
			if let Some(kind) = Key::kind(&key) {
				if should_exclude_from_cdc(kind) {
					continue;
				}

				// Row deltas → try to decode into columnar Diff, fall back to SystemChange
				if kind == KeyKind::Row {
					if let Some(row_key) = RowKey::decode(&key) {
						let decoded = match &delta {
							Delta::Set {
								key,
								values,
							} => {
								let pre = self
									.transaction_store
									.get_previous_version(key, version)
									.ok()
									.flatten();
								if let Some(prev) = pre {
									super::decode::build_update_diff(
										registry,
										row_key.row,
										prev.values,
										values.clone(),
									)
								} else {
									super::decode::build_insert_diff(
										registry,
										row_key.row,
										values.clone(),
									)
								}
							}
							Delta::Unset {
								values,
								..
							} => {
								if !values.is_empty() {
									super::decode::build_remove_diff(
										registry,
										row_key.row,
										values.clone(),
									)
								} else {
									None
								}
							}
							_ => None,
						};

						if let Some(diff) = decoded {
							diffs_by_primitive
								.entry(row_key.primitive)
								.or_default()
								.push(diff);
							continue;
						}
					}
					// Fall through to SystemChange if decode failed
				}
			}

			// Non-row deltas (or row deltas that failed to decode) → SystemChange
			let change = match delta {
				Delta::Set {
					key,
					values,
				} => {
					let pre = self
						.transaction_store
						.get_previous_version(&key, version)
						.ok()
						.flatten();

					if let Some(prev_values) = pre {
						SystemChange::Update {
							key,
							pre: prev_values.values,
							post: values,
						}
					} else {
						SystemChange::Insert {
							key,
							post: values,
						}
					}
				}
				Delta::Unset {
					key,
					values,
				} => {
					let pre = if values.is_empty() {
						None
					} else {
						Some(values)
					};
					SystemChange::Delete {
						key,
						pre,
					}
				}
				Delta::Remove {
					..
				}
				| Delta::Drop {
					..
				} => {
					continue;
				}
			};

			system_changes.push(change);
		}

		// Merge diffs by (PrimitiveId, DiffKind) into batched Changes
		let mut changes: Vec<Change> = Vec::new();
		for (primitive, diffs) in diffs_by_primitive {
			let merged = merge_diffs(diffs);
			changes.push(Change::from_primitive(primitive, version, merged));
		}

		if !changes.is_empty() || !system_changes.is_empty() {
			let cdc = Cdc::new(version, timestamp, changes, system_changes.clone());
			if let Err(e) = self.storage.write(&cdc) {
				error!(version = version.0, "CDC write failed: {:?}", e);
				return;
			}

			debug!(version = version.0, "CDC written successfully");

			// Emit CDC stats event
			let entries: Vec<CdcEntryStats> = system_changes
				.iter()
				.map(|sys_change| {
					let key = sys_change.key();
					let value_bytes = sys_change.value_bytes() as u64;
					CdcEntryStats {
						key: key.clone(),
						value_bytes,
					}
				})
				.collect();

			self.event_bus.emit(CdcStatsRecordedEvent::new(entries, version));
		}
	}

	fn try_cleanup(&self) {
		let result: reifydb_type::Result<()> = (|| {
			let mut txn = self.host.begin_command()?;
			let watermark = compute_watermark(&mut Transaction::Command(&mut txn))?;
			txn.rollback()?;

			let result = self.storage.drop_before(watermark)?;
			if result.count > 0 {
				debug!(watermark = watermark.0, deleted = result.count, "CDC cleanup completed");

				let drop_entries: Vec<CdcEntryDrop> = result
					.entries
					.into_iter()
					.map(|e| CdcEntryDrop {
						key: e.key,
						value_bytes: e.value_bytes,
					})
					.collect();

				self.event_bus.emit(CdcStatsDroppedEvent::new(drop_entries, watermark));
			}
			Ok(())
		})();

		if let Err(e) = result {
			error!("CDC cleanup failed: {:?}", e);
		}
	}
}

/// Merge diffs that share the same variant (Insert/Update/Remove) by appending
/// their `Columns`. Different kinds remain as separate diffs in the output.
pub(crate) fn merge_diffs(diffs: Vec<Diff>) -> Vec<Diff> {
	let mut insert: Option<Diff> = None;
	let mut update: Option<Diff> = None;
	let mut remove: Option<Diff> = None;

	for diff in diffs {
		match diff {
			Diff::Insert {
				post,
			} => {
				if let Some(Diff::Insert {
					post: ref mut existing,
				}) = insert
				{
					if let Err(e) = existing.append_columns(post) {
						error!("Failed to merge insert columns: {:?}", e);
					}
				} else {
					insert = Some(Diff::Insert {
						post,
					});
				}
			}
			Diff::Update {
				pre,
				post,
			} => {
				if let Some(Diff::Update {
					pre: ref mut existing_pre,
					post: ref mut existing_post,
				}) = update
				{
					if let Err(e) = existing_pre.append_columns(pre) {
						error!("Failed to merge update pre columns: {:?}", e);
					}
					if let Err(e) = existing_post.append_columns(post) {
						error!("Failed to merge update post columns: {:?}", e);
					}
				} else {
					update = Some(Diff::Update {
						pre,
						post,
					});
				}
			}
			Diff::Remove {
				pre,
			} => {
				if let Some(Diff::Remove {
					pre: ref mut existing,
				}) = remove
				{
					if let Err(e) = existing.append_columns(pre) {
						error!("Failed to merge remove columns: {:?}", e);
					}
				} else {
					remove = Some(Diff::Remove {
						pre,
					});
				}
			}
		}
	}

	let mut result = Vec::new();
	if let Some(d) = insert {
		result.push(d);
	}
	if let Some(d) = update {
		result.push(d);
	}
	if let Some(d) = remove {
		result.push(d);
	}
	result
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
	type Message = CdcProduceMsg;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		debug!("CDC producer actor started");
		let timer_handle = ctx.schedule_repeat(CLEANUP_INTERVAL, CdcProduceMsg::Tick);
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
			CdcProduceMsg::Produce {
				version,
				timestamp,
				deltas,
			} => {
				self.process(version, timestamp, deltas);
			}
			CdcProduceMsg::Tick => {
				self.try_cleanup();
			}
		}

		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("CDC producer actor stopped");
	}

	fn config(&self) -> ActorConfig {
		// Use a larger mailbox for CDC events which can come in bursts
		ActorConfig::new().mailbox_capacity(256)
	}
}

/// Event listener that forwards PostCommitEvent to the CDC producer actor.
pub struct CdcProducerEventListener {
	actor_ref: ActorRef<CdcProduceMsg>,
	clock: Clock,
}

impl CdcProducerEventListener {
	pub fn new(actor_ref: ActorRef<CdcProduceMsg>, clock: Clock) -> Self {
		Self {
			actor_ref,
			clock,
		}
	}
}

impl EventListener<PostCommitEvent> for CdcProducerEventListener {
	fn on(&self, event: &PostCommitEvent) {
		let msg = CdcProduceMsg::Produce {
			version: *event.version(),
			timestamp: self.clock.now_millis(),
			deltas: event.deltas().iter().cloned().collect(),
		};

		if let Err(e) = self.actor_ref.send(msg) {
			error!("Failed to send CDC event to producer actor: {:?}", e);
		}
	}
}

/// Spawn a CDC producer actor on the given actor system.
///
/// Returns a handle to the actor. The actor_ref from this handle should be used
/// to create a `CdcProducerEventListener` which is then registered on the EventBus.
pub fn spawn_cdc_producer<S, T, H>(
	system: &ActorSystem,
	storage: S,
	transaction_store: T,
	host: H,
	event_bus: EventBus,
) -> ActorHandle<CdcProduceMsg>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
{
	let actor = CdcProducerActor::new(storage, transaction_store, host, event_bus);
	system.spawn("cdc-producer", actor)
}

#[cfg(test)]
pub mod tests {
	use std::{thread::sleep, time::Duration};

	use reifydb_core::encoded::{encoded::EncodedValues, key::EncodedKey};
	use reifydb_runtime::{SharedRuntimeConfig, actor::system::ActorSystem, clock::Clock};
	use reifydb_store_multi::MultiStore;
	use reifydb_store_single::SingleStore;
	use reifydb_transaction::{
		interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction,
		single::SingleTransaction, transaction::command::CommandTransaction,
	};
	use reifydb_type::util::cowvec::CowVec;

	use super::*;
	use crate::storage::memory::MemoryCdcStorage;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_values(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	#[derive(Clone)]
	struct TestCdcHost {
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
		schema_registry: reifydb_catalog::schema::SchemaRegistry,
	}

	impl TestCdcHost {
		fn new() -> Self {
			let multi_store = MultiStore::testing_memory();
			let single_store = SingleStore::testing_memory();
			let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
			let event_bus = EventBus::new(&actor_system);
			let single = SingleTransaction::new(single_store, event_bus.clone());
			let multi = MultiTransaction::new(
				multi_store,
				single.clone(),
				event_bus.clone(),
				actor_system,
				Clock::default(),
			)
			.unwrap();
			Self {
				multi,
				single,
				event_bus,
				schema_registry: reifydb_catalog::schema::SchemaRegistry::testing(),
			}
		}
	}

	impl CdcHost for TestCdcHost {
		fn begin_command(&self) -> reifydb_type::Result<CommandTransaction> {
			CommandTransaction::new(
				self.multi.clone(),
				self.single.clone(),
				self.event_bus.clone(),
				Interceptors::new(),
			)
		}

		fn begin_query(
			&self,
		) -> reifydb_type::Result<reifydb_transaction::transaction::query::QueryTransaction> {
			Ok(reifydb_transaction::transaction::query::QueryTransaction::new(
				self.multi.begin_query()?,
				self.single.clone(),
			))
		}

		fn current_version(&self) -> reifydb_type::Result<CommitVersion> {
			Ok(CommitVersion(1))
		}

		fn done_until(&self) -> CommitVersion {
			CommitVersion(1)
		}

		fn wait_for_mark_timeout(&self, _version: CommitVersion, _timeout: Duration) -> bool {
			true
		}

		fn schema_registry(&self) -> &reifydb_catalog::schema::SchemaRegistry {
			&self.schema_registry
		}
	}

	#[test]
	fn test_producer_processes_insert() {
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::new();
		let handle = spawn_cdc_producer(&actor_system, storage.clone(), resolver, host, event_bus);

		let deltas = vec![Delta::Set {
			key: make_key("test_key"),
			values: make_values("test_value"),
		}];

		handle.actor_ref()
			.send(CdcProduceMsg::Produce {
				version: CommitVersion(1),
				timestamp: 12345,
				deltas,
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
		let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::new();
		let handle = spawn_cdc_producer(&actor_system, storage.clone(), resolver, host, event_bus);

		let deltas = vec![
			Delta::Set {
				key: make_key("key1"),
				values: make_values("value1"),
			},
			Delta::Drop {
				key: make_key("key2"),
				up_to_version: None,
				keep_last_versions: None,
			},
		];

		handle.actor_ref()
			.send(CdcProduceMsg::Produce {
				version: CommitVersion(2),
				timestamp: 12345,
				deltas,
			})
			.unwrap();

		sleep(Duration::from_millis(50));

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		// Only the Set should produce CDC, not the Drop
		assert_eq!(cdc.system_changes.len(), 1);
	}
}
