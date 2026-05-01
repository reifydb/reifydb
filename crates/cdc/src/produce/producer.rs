// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use reifydb_catalog::materialized::MaterializedCatalog;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
	event::{
		EventBus, EventListener,
		metric::{CdcEvictedEvent, CdcEviction, CdcWrite, CdcWrittenEvent},
		transaction::PostCommitEvent,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			shape::ShapeId,
		},
		cdc::{Cdc, SystemChange},
		change::{Change, Diff},
		store::MultiVersionGetPrevious,
	},
	key::{
		EncodableKey, Key, cdc_exclude::should_exclude_from_cdc, kind::KeyKind, row::RowKey,
		series_row::SeriesRowKey,
	},
	util::slab::Slab,
	value::column::{buffer::pool::ColumnBufferPool, columns::Columns},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSystem},
		timers::TimerHandle,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_type::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};
use tracing::{debug, error, trace};

use super::decode::{
	build_insert_diff_into_with_pool, build_remove_diff_into_with_pool, build_update_diff_into_with_pool,
};
use crate::{consume::host::CdcHost, produce::watermark::CdcProducerWatermark, storage::CdcStorage};

/// Cap on how many `Columns` slabs `CdcProducerActor` keeps in its reuse
/// pool. With ~5 KB per slab the cap bounds steady-state pool memory at
/// ~1.3 MB while still satisfying typical per-call burst demand.
const MAX_POOL_SLABS: usize = 256;

/// Default interval between CDC cleanup attempts (30 seconds)
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

use reifydb_core::actors::cdc::{CdcProduceHandle, CdcProduceMessage};

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
	clock: Clock,
	/// Reuse pool of `Columns` slabs. The actor pulls slabs at the start
	/// of each `process()` row-delta iteration, fills them via
	/// `Columns::reset_from_row`, hands `Arc::clone`s into the resulting
	/// `Diff`s for dispatch, and pushes the original `Arc`s back here at
	/// the end of `process()` once dispatch has completed and consumer
	/// references have dropped.
	slab_pool: Slab<Columns>,
	/// Per-`Type` reuse pool of inner `ColumnBuffer`s. Threaded through
	/// the `_with_pool` decode helpers so a slab whose previous shape
	/// differs from the new row's shape can swap out individual column
	/// buffers without allocating a fresh inner `Vec` per column.
	pool: ColumnBufferPool,
	watermark: CdcProducerWatermark,
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
	) -> Self {
		Self {
			storage: Arc::new(storage),
			transaction_store: Arc::new(transaction_store),
			host,
			event_bus,
			clock,
			slab_pool: Slab::new(MAX_POOL_SLABS),
			pool: ColumnBufferPool::new(),
			watermark,
		}
	}

	fn process(&self, version: CommitVersion, changed_at: DateTime, deltas: Vec<Delta>) {
		let mut diffs_by_shape: BTreeMap<ShapeId, Vec<Diff>> = BTreeMap::new();
		let mut system_changes: Vec<SystemChange> = Vec::new();
		// Slabs pulled from the pool for this call. Each entry is an
		// `Arc<Columns>` that was filled in place by `build_*_diff_into`
		// and Arc-cloned into a `Diff`. After dispatch we release them
		// back to the pool; their `strong_count` will be 1 once the
		// dispatched `Diff`s have been dropped.
		let mut acquired_slabs: Vec<Arc<Columns>> = Vec::new();
		let catalog = self.host.materialized_catalog();

		trace!(version = version.0, delta_count = deltas.len(), "Processing CDC");

		for delta in deltas {
			if Self::is_excluded_kind(&delta) {
				continue;
			}
			if self.try_decode_row_delta(
				&delta,
				version,
				catalog,
				&mut diffs_by_shape,
				&mut system_changes,
				&mut acquired_slabs,
			) {
				continue;
			}
			if let Some(change) = self.delta_to_system_change(delta, version) {
				system_changes.push(change);
			}
		}

		let changes = self.merge_into_changes(diffs_by_shape, version, changed_at);
		self.write_and_emit(version, changed_at, changes, system_changes);
		self.release_slabs(acquired_slabs);
	}

	#[inline]
	fn is_excluded_kind(delta: &Delta) -> bool {
		Key::kind(delta.key()).map(should_exclude_from_cdc).unwrap_or(false)
	}

	#[inline]
	fn try_decode_row_delta(
		&self,
		delta: &Delta,
		version: CommitVersion,
		catalog: &MaterializedCatalog,
		diffs_by_shape: &mut BTreeMap<ShapeId, Vec<Diff>>,
		system_changes: &mut Vec<SystemChange>,
		acquired_slabs: &mut Vec<Arc<Columns>>,
	) -> bool {
		let key = delta.key();
		if Key::kind(key) != Some(KeyKind::Row) {
			return false;
		}

		let (shape, row_number) = if let Some(sk) = SeriesRowKey::decode(key) {
			(ShapeId::Series(sk.series), RowNumber::from(sk.sequence))
		} else if let Some(rk) = RowKey::decode(key) {
			(rk.shape, rk.row)
		} else {
			return false;
		};

		let Some(diff) = self.build_row_diff(delta, row_number, version, catalog, acquired_slabs) else {
			return false;
		};
		diffs_by_shape.entry(shape).or_default().push(diff);
		push_raw_system_change(delta, self.transaction_store.as_ref(), version, system_changes);
		true
	}

	#[inline]
	fn build_row_diff(
		&self,
		delta: &Delta,
		row_number: RowNumber,
		version: CommitVersion,
		catalog: &MaterializedCatalog,
		acquired_slabs: &mut Vec<Arc<Columns>>,
	) -> Option<Diff> {
		match delta {
			Delta::Set {
				key,
				row,
			} => self.build_diff_for_set(key, row, row_number, version, catalog, acquired_slabs),
			Delta::Unset {
				row,
				..
			} if !row.is_empty() => self.build_diff_for_unset(row, row_number, catalog, acquired_slabs),
			_ => None,
		}
	}

	#[inline]
	fn build_diff_for_set(
		&self,
		key: &EncodedKey,
		row: &EncodedRow,
		row_number: RowNumber,
		version: CommitVersion,
		catalog: &MaterializedCatalog,
		acquired_slabs: &mut Vec<Arc<Columns>>,
	) -> Option<Diff> {
		let pre = self.transaction_store.get_previous_version(key, version).ok().flatten();
		if let Some(prev) = pre {
			let mut pre_buf = self.slab_pool.acquire();
			let mut post_buf = self.slab_pool.acquire();
			let diff = build_update_diff_into_with_pool(
				catalog,
				row_number,
				prev.row,
				row.clone(),
				&mut pre_buf,
				&mut post_buf,
				&self.pool,
			);
			acquired_slabs.push(pre_buf);
			acquired_slabs.push(post_buf);
			diff
		} else {
			let mut post_buf = self.slab_pool.acquire();
			let diff = build_insert_diff_into_with_pool(
				catalog,
				row_number,
				row.clone(),
				&mut post_buf,
				&self.pool,
			);
			acquired_slabs.push(post_buf);
			diff
		}
	}

	#[inline]
	fn build_diff_for_unset(
		&self,
		row: &EncodedRow,
		row_number: RowNumber,
		catalog: &MaterializedCatalog,
		acquired_slabs: &mut Vec<Arc<Columns>>,
	) -> Option<Diff> {
		let mut pre_buf = self.slab_pool.acquire();
		let diff = build_remove_diff_into_with_pool(catalog, row_number, row.clone(), &mut pre_buf, &self.pool);
		acquired_slabs.push(pre_buf);
		diff
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
	fn merge_into_changes(
		&self,
		diffs_by_shape: BTreeMap<ShapeId, Vec<Diff>>,
		version: CommitVersion,
		changed_at: DateTime,
	) -> Vec<Change> {
		let mut changes = Vec::with_capacity(diffs_by_shape.len());
		for (shape, diffs) in diffs_by_shape {
			let merged = merge_diffs(diffs);
			changes.push(Change::from_shape(shape, version, merged, changed_at));
		}
		changes
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
		// `cdc` (and therefore the dispatched `Diff`s and their `Arc<Columns>`
		// clones) drops here, returning each acquired slab's strong_count to 1.
		// After this point each slab in `acquired_slabs` has strong_count == 1
		// unless an event-bus consumer kept its own clone alive - in which case
		// `Slab::acquire` will skip it on a future pop.
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

	#[inline]
	fn release_slabs(&self, slabs: Vec<Arc<Columns>>) {
		for slab in slabs {
			self.slab_pool.release(slab);
		}
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

	/// Returns the cutoff version below which all CDC entries should be evicted,
	/// or `None` if eviction should be skipped (no TTL configured, no entries
	/// match, or cutoff_version is zero).
	#[inline]
	fn find_eviction_target(&self) -> Result<Option<CommitVersion>> {
		let Some(ttl) = self.host.materialized_catalog().get_config_duration_opt(ConfigKey::CdcTtlDuration)
		else {
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
	fn on_produce(&self, version: CommitVersion, changed_at: DateTime, deltas: Vec<Delta>) {
		self.process(version, changed_at, deltas);
		// Advance the watermark AFTER process completes (whether or not a CDC
		// entry was written). The compactor reads this watermark as a hard cap
		// to guarantee that no later producer write can land at a version
		// already covered by a packed block.
		self.watermark.advance(version);
	}

	#[inline]
	fn on_tick(&self) {
		self.try_cleanup();
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
	if let Some(change) = delta_to_raw_system_change(delta, transaction_store, version) {
		system_changes.push(change);
	}
}

/// Build a `SystemChange` for the part of `delta` that is part of CDC's raw
/// stream (Set, Unset). `Delta::Remove` and `Delta::Drop` are handled by the
/// caller; this helper covers only the shape both `push_raw_system_change`
/// and `delta_to_system_change` agree on.
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

/// Merge diffs that share the same variant (Insert/Update/Remove) by appending
/// their `Columns`. Different kinds remain as separate diffs in the output.
pub(crate) fn merge_diffs(diffs: Vec<Diff>) -> Vec<Diff> {
	let mut insert_post: Option<Arc<Columns>> = None;
	let mut update_pre: Option<Arc<Columns>> = None;
	let mut update_post: Option<Arc<Columns>> = None;
	let mut remove_pre: Option<Arc<Columns>> = None;

	for diff in diffs {
		match diff {
			Diff::Insert {
				post,
			} => merge_or_init(&mut insert_post, post, "insert"),
			Diff::Update {
				pre,
				post,
			} => {
				merge_or_init(&mut update_pre, pre, "update pre");
				merge_or_init(&mut update_post, post, "update post");
			}
			Diff::Remove {
				pre,
			} => merge_or_init(&mut remove_pre, pre, "remove"),
		}
	}

	let mut result = Vec::with_capacity(3);
	if let Some(post) = insert_post {
		result.push(Diff::Insert {
			post,
		});
	}
	if let (Some(pre), Some(post)) = (update_pre, update_post) {
		result.push(Diff::Update {
			pre,
			post,
		});
	}
	if let Some(pre) = remove_pre {
		result.push(Diff::Remove {
			pre,
		});
	}
	result
}

#[inline]
fn merge_or_init(slot: &mut Option<Arc<Columns>>, fresh: Arc<Columns>, ctx: &str) {
	match slot {
		Some(existing) => {
			let owned = Arc::try_unwrap(fresh).unwrap_or_else(|arc| (*arc).clone());
			if let Err(e) = Arc::make_mut(existing).append_columns(owned) {
				error!("Failed to merge {ctx} columns: {:?}", e);
			}
		}
		None => *slot = Some(fresh),
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
			} => self.on_produce(version, changed_at, deltas),
			CdcProduceMessage::Tick => self.on_tick(),
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
	clock: Clock,
	watermark: CdcProducerWatermark,
) -> CdcProduceHandle
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
{
	let actor = CdcProducerActor::new(storage, transaction_store, host, event_bus, clock, watermark);
	system.spawn_system("cdc-producer", actor)
}

#[cfg(test)]
pub mod tests {
	use std::{thread::sleep, time::Duration};

	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, pool::Pools};
	use reifydb_store_multi::MultiStore;
	use reifydb_type::value::datetime::DateTime;

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
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::new();
		let clock = host.clock.clone();
		let handle = spawn_cdc_producer(
			&actor_system,
			storage.clone(),
			resolver,
			host,
			event_bus,
			clock,
			CdcProducerWatermark::new(),
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
		let event_bus = EventBus::new(&actor_system);
		let host = TestCdcHost::new();
		let clock = host.clock.clone();
		let handle = spawn_cdc_producer(
			&actor_system,
			storage.clone(),
			resolver,
			host,
			event_bus,
			clock,
			CdcProducerWatermark::new(),
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
			})
			.unwrap();

		sleep(Duration::from_millis(50));

		let cdc = storage.read(CommitVersion(2)).unwrap().unwrap();
		// Only the Set should produce CDC, not the Drop
		assert_eq!(cdc.system_changes.len(), 1);
	}
}

// TTL behaviour is exercised by the integration suite at `crates/cdc/tests/ttl.rs`.
