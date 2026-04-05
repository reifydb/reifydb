// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::{
		EventBus, EventListener,
		metric::{CdcEntryDrop, CdcEntryStats, CdcStatsDroppedEvent, CdcStatsRecordedEvent},
		transaction::PostCommitEvent,
	},
	interface::{
		catalog::shape::ShapeId,
		cdc::{Cdc, SystemChange},
		change::{Change, Diff},
		store::MultiVersionGetPrevious,
	},
	key::{
		EncodableKey, Key, cdc_exclude::should_exclude_from_cdc, kind::KeyKind, row::RowKey,
		series_row::SeriesRowKey,
	},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorHandle, ActorSystem},
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};
use tracing::{debug, error, trace};

use super::decode::{build_insert_diff, build_remove_diff, build_update_diff};
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
		changed_at: DateTime,
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

	fn process(&self, version: CommitVersion, changed_at: DateTime, deltas: Vec<Delta>) {
		let mut diffs_by_shape: BTreeMap<ShapeId, Vec<Diff>> = BTreeMap::new();
		let mut system_changes: Vec<SystemChange> = Vec::new();
		let catalog = self.host.materialized_catalog();

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
					// Try series key first (more specific encoding)
					if let Some(series_key) = SeriesRowKey::decode(&key) {
						let shape = ShapeId::Series(series_key.series);
						let row_number = RowNumber::from(series_key.sequence);
						let decoded = match &delta {
							Delta::Set {
								key,
								row,
							} => {
								let pre = self
									.transaction_store
									.get_previous_version(key, version)
									.ok()
									.flatten();
								if let Some(prev) = pre {
									build_update_diff(
										catalog,
										row_number,
										prev.row,
										row.clone(),
									)
								} else {
									build_insert_diff(
										catalog,
										row_number,
										row.clone(),
									)
								}
							}
							Delta::Unset {
								row,
								..
							} => {
								if !row.is_empty() {
									build_remove_diff(
										catalog,
										row_number,
										row.clone(),
									)
								} else {
									None
								}
							}
							_ => None,
						};
						if let Some(diff) = decoded {
							diffs_by_shape.entry(shape).or_default().push(diff);
							// Also keep as SystemChange for replication
							push_raw_system_change(
								&delta,
								self.transaction_store.as_ref(),
								version,
								&mut system_changes,
							);
							continue;
						}
					}

					// Table/view/ringbuffer row key
					if let Some(row_key) = RowKey::decode(&key) {
						let decoded = match &delta {
							Delta::Set {
								key,
								row,
							} => {
								let pre = self
									.transaction_store
									.get_previous_version(key, version)
									.ok()
									.flatten();
								if let Some(prev) = pre {
									build_update_diff(
										catalog,
										row_key.row,
										prev.row,
										row.clone(),
									)
								} else {
									build_insert_diff(
										catalog,
										row_key.row,
										row.clone(),
									)
								}
							}
							Delta::Unset {
								row,
								..
							} => {
								if !row.is_empty() {
									build_remove_diff(
										catalog,
										row_key.row,
										row.clone(),
									)
								} else {
									None
								}
							}
							_ => None,
						};

						if let Some(diff) = decoded {
							diffs_by_shape.entry(row_key.shape).or_default().push(diff);
							// Also keep as SystemChange for replication
							push_raw_system_change(
								&delta,
								self.transaction_store.as_ref(),
								version,
								&mut system_changes,
							);
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
					row,
				} => {
					let pre = self
						.transaction_store
						.get_previous_version(&key, version)
						.ok()
						.flatten();

					if let Some(prev_values) = pre {
						SystemChange::Update {
							key,
							pre: prev_values.row,
							post: row,
						}
					} else {
						SystemChange::Insert {
							key,
							post: row,
						}
					}
				}
				Delta::Unset {
					key,
					row,
				} => {
					let pre = if row.is_empty() {
						None
					} else {
						Some(row)
					};
					SystemChange::Delete {
						key,
						pre,
					}
				}
				Delta::Remove {
					key,
				} => SystemChange::Delete {
					key,
					pre: None,
				},
				Delta::Drop {
					..
				} => {
					continue;
				}
			};

			system_changes.push(change);
		}

		// Merge diffs by (ShapeId, DiffKind) into batched Changes
		let mut changes: Vec<Change> = Vec::new();
		for (shape, diffs) in diffs_by_shape {
			let merged = merge_diffs(diffs);
			changes.push(Change::from_shape(shape, version, merged, changed_at));
		}

		if !changes.is_empty() || !system_changes.is_empty() {
			let cdc = Cdc::new(version, changed_at, changes, system_changes.clone());
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
		let result: Result<()> = (|| {
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

/// Push a raw SystemChange for a delta, preserving the encoded key-value
/// data alongside the decoded columnar form. This enables replication consumers
/// to access the original bytes without re-encoding.
fn push_raw_system_change(
	delta: &Delta,
	transaction_store: &dyn MultiVersionGetPrevious,
	version: CommitVersion,
	system_changes: &mut Vec<SystemChange>,
) {
	let change = match delta {
		Delta::Set {
			key,
			row,
		} => {
			let pre = transaction_store.get_previous_version(key, version).ok().flatten();
			if let Some(prev) = pre {
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
			}
		}
		Delta::Unset {
			key,
			row,
		} => {
			let pre = if row.is_empty() {
				None
			} else {
				Some(row.clone())
			};
			SystemChange::Delete {
				key: key.clone(),
				pre,
			}
		}
		_ => return,
	};
	system_changes.push(change);
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
				changed_at,
				deltas,
			} => {
				self.process(version, changed_at, deltas);
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
			changed_at: DateTime::from_nanos(self.clock.now_nanos()),
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

	use reifydb_catalog::materialized::MaterializedCatalog;
	use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow};
	use reifydb_runtime::{
		actor::system::ActorSystem,
		context::{
			clock::{Clock, MockClock},
			rng::Rng,
		},
	};
	use reifydb_store_multi::MultiStore;
	use reifydb_store_single::SingleStore;
	use reifydb_transaction::{
		interceptor::interceptors::Interceptors,
		multi::transaction::MultiTransaction,
		single::SingleTransaction,
		transaction::{command::CommandTransaction, query::QueryTransaction},
	};
	use reifydb_type::{
		util::cowvec::CowVec,
		value::{datetime::DateTime, identity::IdentityId},
	};

	use super::*;
	use crate::storage::memory::MemoryCdcStorage;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_row(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	#[derive(Clone)]
	struct TestCdcHost {
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
		materialized_catalog: MaterializedCatalog,
		clock: Clock,
	}

	impl TestCdcHost {
		fn new() -> Self {
			let multi_store = MultiStore::testing_memory();
			let single_store = SingleStore::testing_memory();
			let actor_system = ActorSystem::new(1);
			let event_bus = EventBus::new(&actor_system);
			let single = SingleTransaction::new(single_store, event_bus.clone());
			let materialized_catalog = MaterializedCatalog::new();
			let clock = Clock::Mock(MockClock::from_millis(1000));
			let multi = MultiTransaction::new(
				multi_store,
				single.clone(),
				event_bus.clone(),
				actor_system,
				clock.clone(),
				Rng::seeded(42),
				Arc::new(materialized_catalog.clone()),
			)
			.unwrap();
			Self {
				multi,
				single,
				event_bus,
				materialized_catalog,
				clock,
			}
		}
	}

	impl CdcHost for TestCdcHost {
		fn begin_command(&self) -> Result<CommandTransaction> {
			CommandTransaction::new(
				self.multi.clone(),
				self.single.clone(),
				self.event_bus.clone(),
				Interceptors::new(),
				IdentityId::system(),
				self.clock.clone(),
			)
		}

		fn begin_query(&self) -> Result<QueryTransaction> {
			Ok(QueryTransaction::new(self.multi.begin_query()?, self.single.clone(), IdentityId::system()))
		}

		fn current_version(&self) -> Result<CommitVersion> {
			Ok(CommitVersion(1))
		}

		fn done_until(&self) -> CommitVersion {
			CommitVersion(1)
		}

		fn wait_for_mark_timeout(&self, _version: CommitVersion, _timeout: Duration) -> bool {
			true
		}

		fn materialized_catalog(&self) -> &MaterializedCatalog {
			&self.materialized_catalog
		}
	}

	#[test]
	fn test_producer_processes_insert() {
		let storage = MemoryCdcStorage::new();
		let store = MultiStore::testing_memory();
		let resolver = store;
		let actor_system = ActorSystem::new(1);
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::new();
		let handle = spawn_cdc_producer(&actor_system, storage.clone(), resolver, host, event_bus);

		let deltas = vec![Delta::Set {
			key: make_key("test_key"),
			row: make_row("test_value"),
		}];

		handle.actor_ref()
			.send(CdcProduceMsg::Produce {
				version: CommitVersion(1),
				changed_at: DateTime::from_nanos(12345000),
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
		let actor_system = ActorSystem::new(1);
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::new();
		let handle = spawn_cdc_producer(&actor_system, storage.clone(), resolver, host, event_bus);

		let deltas = vec![
			Delta::Set {
				key: make_key("key1"),
				row: make_row("value1"),
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
				changed_at: DateTime::from_nanos(12345000),
				deltas,
			})
			.unwrap();

		sleep(Duration::from_millis(50));

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		// Only the Set should produce CDC, not the Drop
		assert_eq!(cdc.system_changes.len(), 1);
	}
}
