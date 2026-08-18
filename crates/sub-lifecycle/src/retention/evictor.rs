// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::shape::RowFamily,
};
use reifydb_core::{
	event::row::RowsExpiredEvent,
	interface::{
		WithEventBus,
		catalog::{
			config::{ConfigKey, GetConfig},
			id::{RingBufferId, SeriesId, TableId},
			storage::StorageId,
		},
		store::classify_range,
	},
	key::{
		EncodableKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::RowKey,
		series_row::SeriesRowKeyRange,
	},
	lifecycle::{
		class::{Floor, FloorTerm, RetentionClass},
		metrics::RetentionMetrics,
		progress::Progress,
		task::LifecycleTask,
	},
	row::Ttl,
	state::horizon::Cutoff,
};
use reifydb_engine::{
	engine::StandardEngine,
	transaction::operation::{
		ringbuffer::apply_ringbuffer_partition_metadata_after_delete,
		series::apply_series_metadata_after_delete,
	},
};
use reifydb_store_multi::{MultiStore, store::StandardMultiStore};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{
	Result,
	value::{Value, datetime::DateTime, duration::Duration, identity::IdentityId, partition::Partition},
};
use tracing::{debug, instrument, warn};

use crate::{
	plane::RetentionPlane,
	retention::{scan, scan::ExpiryCursor},
};

type CursorKey = (StorageId, EncodedKey);

const EVICT_CONSECUTIVE_FAILURE_LIMIT: u32 = 5;

#[derive(Default)]
pub struct EvictorState {
	running: bool,
	cursors: HashMap<CursorKey, EncodedKey>,
	expiry_cursors: HashMap<CursorKey, ExpiryCursor>,
	resume: Option<StorageId>,
	failures: HashMap<StorageId, u32>,
}

impl EvictorState {
	fn forget(&mut self, storage: StorageId) {
		self.cursors.retain(|key, _| key.0 != storage);
		self.expiry_cursors.retain(|key, _| key.0 != storage);
	}
}

#[derive(Default)]
struct ClassTally {
	rows: u64,
	backlog: u64,
}

#[derive(Default)]
struct TickStats {
	objects_scanned: u64,
	objects_skipped: u64,
	rows_expired: u64,
}

pub struct Evictor {
	engine: StandardEngine,
	plane: RetentionPlane,
	store: StandardMultiStore,
}

impl Evictor {
	pub fn new(engine: StandardEngine) -> Self {
		let plane = RetentionPlane::for_engine(&engine, RetentionMetrics::new());
		Self::with_plane(engine, plane)
	}

	pub fn with_plane(engine: StandardEngine, plane: RetentionPlane) -> Self {
		let store = match engine.multi_owned().store() {
			MultiStore::Standard(store) => store.clone(),
		};
		Self {
			engine,
			plane,
			store,
		}
	}

	pub fn plane(&self) -> &RetentionPlane {
		&self.plane
	}

	#[instrument(name = "lifecycle::retention::evict::tick", level = "debug", skip_all, fields(class = %target))]
	fn run_tick(&self, state: &mut EvictorState, target: RetentionClass, now: DateTime) -> Progress {
		if state.running {
			debug!("retention eviction tick already in progress, skipping");
			return Progress::Exhausted;
		}
		state.running = true;

		let catalog = self.engine.catalog();
		let batch_size = catalog.get_config_uint8(ConfigKey::RetentionEvictBatchSize) as usize;
		let mut budget = catalog.get_config_uint8(ConfigKey::RetentionEvictMaxBatchesPerTick);
		let mut stats = TickStats::default();

		let mut tally = ClassTally::default();
		let mut unvisited_eligible = false;

		let mut eligible: Vec<(StorageId, Ttl)> = catalog
			.list_row_settings()
			.into_iter()
			.filter_map(|(storage, settings)| {
				let ttl = settings.ttl?;
				(RetentionClass::RowTtl == target).then_some((storage, ttl))
			})
			.collect();

		eligible.sort_unstable_by_key(|(storage, _)| *storage);

		let start = state
			.resume
			.and_then(|last| eligible.iter().position(|(storage, _)| *storage > last))
			.unwrap_or(0);
		let mut resume_after = None;

		for offset in 0..eligible.len() {
			if budget == 0 {
				unvisited_eligible = true;
				break;
			}
			let (storage, ttl) = &eligible[(start + offset) % eligible.len()];
			let storage = *storage;
			let Some((cutoff, _)) = self.expiry_cutoff(now, ttl) else {
				stats.objects_skipped += 1;
				resume_after = Some(storage);
				continue;
			};

			stats.objects_scanned += 1;
			let expired_before = stats.rows_expired;
			match self.evict_storage(state, storage, cutoff, batch_size, &mut budget, &mut stats) {
				Ok(()) => {
					state.failures.remove(&storage);
				}
				Err(e) => {
					let failures = state.failures.entry(storage).or_default();
					*failures += 1;
					if *failures >= EVICT_CONSECUTIVE_FAILURE_LIMIT {
						panic!(
							"retention eviction failed {failures} consecutive times for storage {storage:?}: {e}"
						);
					}
					warn!(?storage, failures = *failures, error = %e, "retention eviction failed; resetting cursors, retrying next tick");
					state.forget(storage);
					budget = budget.saturating_sub(1);
				}
			}

			tally.rows += stats.rows_expired - expired_before;
			if state.cursors.keys().any(|key| key.0 == storage) {
				tally.backlog += 1;
			}
			resume_after = Some(storage);
		}

		state.resume = if unvisited_eligible {
			resume_after
		} else {
			None
		};

		let backlog = tally.backlog + u64::from(unvisited_eligible);
		self.plane.record_reclamation(target, self.class_floor(&eligible, now), tally.rows, backlog);
		let budget_exhausted = budget == 0;
		if budget_exhausted {
			self.plane.record_budget_exhausted(target);
		}

		if stats.rows_expired > 0 {
			debug!(
				objects_scanned = stats.objects_scanned,
				objects_skipped = stats.objects_skipped,
				rows_expired = stats.rows_expired,
				"retention eviction tick completed"
			);
		}
		self.engine.event_bus().emit(RowsExpiredEvent::new(
			stats.objects_scanned,
			stats.objects_skipped,
			stats.rows_expired,
			0,
			HashMap::new(),
			HashMap::new(),
		));
		state.running = false;

		if budget_exhausted && backlog > 0 {
			Progress::Yielded
		} else {
			Progress::Exhausted
		}
	}

	fn class_floor(&self, eligible: &[(StorageId, Ttl)], now: DateTime) -> Option<(Floor, FloorTerm)> {
		let mut floor: Option<(Floor, FloorTerm)> = None;
		for (_, ttl) in eligible {
			let Some((cutoff, binding)) = self.expiry_cutoff(now, ttl) else {
				continue;
			};
			floor = Some(match floor {
				Some(held) if held.0.monotonic_key() <= cutoff.raw() => held,
				_ => (Floor::Instant(cutoff.instant()), binding),
			});
		}
		floor
	}

	fn expiry_cutoff(&self, now: DateTime, ttl: &Ttl) -> Option<(Cutoff, FloorTerm)> {
		let (floor, binding) =
			self.plane.cutoff_with_binding(RetentionClass::RowTtl, now, Some(ttl.duration))?;
		Some((Cutoff(floor.instant()?), binding))
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "lifecycle::retention::evict::object", level = "debug", skip_all)]
	fn evict_storage(
		&self,
		state: &mut EvictorState,
		storage: StorageId,
		cutoff: Cutoff,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		match storage {
			StorageId::Table(id) => self.evict_table(state, id, cutoff, batch_size, budget, stats),
			StorageId::View(_) => {
				unreachable!("a view's rows carry its backing object's storage id")
			}
			StorageId::RingBuffer(id) => {
				self.evict_ringbuffer(state, id, cutoff, batch_size, budget, stats)
			}
			StorageId::Series(id) => self.evict_series(state, id, cutoff, batch_size, budget, stats),
			StorageId::Queue(_) => Ok(()),
		}
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "lifecycle::retention::evict::table", level = "debug", skip_all)]
	fn evict_table(
		&self,
		state: &mut EvictorState,
		id: TableId,
		cutoff: Cutoff,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		let storage = StorageId::Table(id);
		for keyspace in [RowKey::full_scan(storage), PartitionedRowKey::full_scan(storage)] {
			loop {
				if *budget == 0 {
					return Ok(());
				}
				let (rows, drained) =
					self.evict_table_batch(state, id, cutoff, batch_size, &keyspace)?;
				stats.rows_expired += rows;
				if rows > 0 || !drained {
					*budget -= 1;
				}
				if drained {
					break;
				}
			}
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn expired_batch(
		&self,
		state: &mut EvictorState,
		txn: &mut CommandTransaction,
		cursor_key: &CursorKey,
		keyspace: &EncodedKeyRange,
		family: RowFamily,
		cutoff: Cutoff,
		batch_size: usize,
	) -> Result<(Vec<EncodedKey>, bool)> {
		if let (Some(persistent), Some(kind)) = (self.store.persistent(), classify_range(keyspace)) {
			let scan = scan::scan_expired_indexed(
				txn,
				persistent,
				kind,
				family,
				cutoff,
				state.expiry_cursors.get(cursor_key),
				batch_size,
			)?;
			let drained = match scan.next_cursor {
				Some(cursor) => {
					state.expiry_cursors.insert(cursor_key.clone(), cursor);
					false
				}
				None => true,
			};
			return Ok((scan.expired, drained));
		}

		let range = scan::resume_range(keyspace, state.cursors.get(cursor_key));
		let result = scan::scan_expired(txn, range, family, cutoff, batch_size, &|_| None)?;
		let drained = advance_cursor(state, cursor_key.clone(), result.next_cursor);
		Ok((result.expired.into_iter().map(|row| row.key).collect(), drained))
	}

	#[instrument(name = "lifecycle::retention::evict::table_batch", level = "trace", skip_all)]
	fn evict_table_batch(
		&self,
		state: &mut EvictorState,
		id: TableId,
		cutoff: Cutoff,
		batch_size: usize,
		keyspace: &EncodedKeyRange,
	) -> Result<(u64, bool)> {
		let storage = StorageId::Table(id);
		let cursor_key = (storage, scan::keyspace_start(keyspace));
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		if catalog.find_table(&mut Transaction::Command(&mut txn), id)?.is_none() {
			txn.rollback()?;
			state.forget(storage);
			return Ok((0, true));
		}

		let (expired, drained) = self.expired_batch(
			state,
			&mut txn,
			&cursor_key,
			keyspace,
			RowFamily::Table,
			cutoff,
			batch_size,
		)?;
		if expired.is_empty() {
			txn.rollback()?;
			return Ok((0, drained));
		}

		let rows = expired.len() as u64;
		for key in &expired {
			txn.remove_silent(key)?;
		}
		txn.commit()?;
		Ok((rows, drained))
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "lifecycle::retention::evict::ringbuffer", level = "debug", skip_all)]
	fn evict_ringbuffer(
		&self,
		state: &mut EvictorState,
		id: RingBufferId,
		cutoff: Cutoff,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		let storage = StorageId::RingBuffer(id);
		for keyspace in [RowKey::full_scan(storage), PartitionedRowKey::full_scan(storage)] {
			loop {
				if *budget == 0 {
					return Ok(());
				}
				let (rows, drained) =
					self.evict_ringbuffer_batch(state, id, cutoff, batch_size, &keyspace)?;
				stats.rows_expired += rows;
				if rows > 0 || !drained {
					*budget -= 1;
				}
				if drained {
					break;
				}
			}
		}
		Ok(())
	}

	#[instrument(name = "lifecycle::retention::evict::ringbuffer_batch", level = "trace", skip_all)]
	fn evict_ringbuffer_batch(
		&self,
		state: &mut EvictorState,
		id: RingBufferId,
		cutoff: Cutoff,
		batch_size: usize,
		keyspace: &EncodedKeyRange,
	) -> Result<(u64, bool)> {
		let storage = StorageId::RingBuffer(id);
		let cursor_key = (storage, scan::keyspace_start(keyspace));
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		let Some(ringbuffer) = catalog.find_ringbuffer(&mut Transaction::Command(&mut txn), id)? else {
			txn.rollback()?;
			state.forget(storage);
			return Ok((0, true));
		};
		if ringbuffer.underlying {
			txn.rollback()?;
			state.forget(storage);
			return Ok((0, true));
		}

		let (expired, drained) = self.expired_batch(
			state,
			&mut txn,
			&cursor_key,
			keyspace,
			RowFamily::RingBuffer,
			cutoff,
			batch_size,
		)?;
		if expired.is_empty() {
			txn.rollback()?;
			return Ok((0, drained));
		}

		let partitioned = !ringbuffer.partition_by.is_empty();
		let mut groups: HashMap<Partition, Vec<EncodedKey>> = HashMap::new();
		for key in &expired {
			let partition = if partitioned {
				let Some(decoded) = PartitionedRowKey::decode(key) else {
					continue;
				};
				decoded.partition
			} else {
				Partition::default()
			};
			groups.entry(partition).or_default().push(key.clone());
		}

		let values_by_partition: HashMap<Partition, Vec<Value>> = if partitioned {
			catalog.list_ringbuffer_partitions(&mut Transaction::Command(&mut txn), &ringbuffer)?
				.into_iter()
				.map(|entry| (Partition::of(&entry.partition_values), entry.partition_values))
				.collect()
		} else {
			HashMap::new()
		};

		let mut evicted = 0u64;
		for (partition, keys) in groups {
			let partition_values: &[Value] = if partitioned {
				let Some(values) = values_by_partition.get(&partition) else {
					continue;
				};
				values
			} else {
				&[]
			};

			let Some(metadata) = catalog.find_partition_metadata(
				&mut Transaction::Command(&mut txn),
				&ringbuffer,
				partition_values,
			)?
			else {
				continue;
			};

			let partition_keyspace = if partitioned {
				PartitionedRowKey::partition_range(storage, partition)
			} else {
				RowKey::full_scan(storage)
			};
			let survivor = scan::min_survivor_row(&mut txn, partition_keyspace, &keys, &|key| {
				decode_ringbuffer_row_number(key, partitioned)
			})?;

			for key in &keys {
				txn.remove_silent(key)?;
			}

			let deleted = keys.len() as u64;
			apply_ringbuffer_partition_metadata_after_delete(
				&catalog,
				&mut Transaction::Command(&mut txn),
				&ringbuffer,
				partition_values,
				metadata,
				deleted,
				survivor,
			)?;
			evicted += deleted;
		}

		txn.commit()?;
		Ok((evicted, drained))
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "lifecycle::retention::evict::series", level = "debug", skip_all)]
	fn evict_series(
		&self,
		state: &mut EvictorState,
		id: SeriesId,
		cutoff: Cutoff,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		loop {
			if *budget == 0 {
				return Ok(());
			}
			let (rows, drained) = self.evict_series_batch(state, id, cutoff, batch_size)?;
			stats.rows_expired += rows;
			if rows > 0 || !drained {
				*budget -= 1;
			}
			if drained {
				return Ok(());
			}
		}
	}

	#[instrument(name = "lifecycle::retention::evict::series_batch", level = "trace", skip_all)]
	fn evict_series_batch(
		&self,
		state: &mut EvictorState,
		id: SeriesId,
		cutoff: Cutoff,
		batch_size: usize,
	) -> Result<(u64, bool)> {
		let storage = StorageId::Series(id);
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		let Some(series) = catalog.find_series(&mut Transaction::Command(&mut txn), id)? else {
			txn.rollback()?;
			state.forget(storage);
			return Ok((0, true));
		};
		let Some(mut metadata) =
			catalog.find_series_metadata(&mut Transaction::Command(&mut txn), series.id)?
		else {
			txn.rollback()?;
			state.forget(storage);
			return Ok((0, true));
		};

		let partitioned = !series.partition_by.is_empty();
		let keyspace = if partitioned {
			PartitionedRowKey::full_scan(storage)
		} else {
			SeriesRowKeyRange::full_scan(series.id, None)
		};
		let cursor_key = (storage, scan::keyspace_start(&keyspace));

		let (expired, drained) = self.expired_batch(
			state,
			&mut txn,
			&cursor_key,
			&keyspace,
			RowFamily::Series,
			cutoff,
			batch_size,
		)?;
		if expired.is_empty() {
			txn.rollback()?;
			return Ok((0, drained));
		}

		let deleted = expired.len() as u64;
		for key in &expired {
			txn.remove_silent(key)?;
		}
		apply_series_metadata_after_delete(&mut metadata, deleted);
		catalog.update_series_metadata_txn(&mut Transaction::Command(&mut txn), series.id, metadata)?;
		txn.commit()?;
		Ok((deleted, drained))
	}
}

fn advance_cursor(state: &mut EvictorState, cursor_key: CursorKey, next: Option<EncodedKey>) -> bool {
	match next {
		Some(cursor) => {
			state.cursors.insert(cursor_key, cursor);
			false
		}
		None => {
			state.cursors.remove(&cursor_key);
			true
		}
	}
}

fn decode_ringbuffer_row_number(key: &EncodedKey, partitioned: bool) -> Option<u64> {
	if partitioned {
		match PartitionedRowKey::decode(key).map(|k| k.locator) {
			Some(RowLocator::Row(row_number)) => Some(row_number.0),
			_ => None,
		}
	} else {
		RowKey::decode(key).map(|k| k.row.0)
	}
}

pub struct RetentionEvictTask {
	evictor: Evictor,
	state: EvictorState,
	class: RetentionClass,
}

impl RetentionEvictTask {
	pub fn silent(engine: StandardEngine, plane: RetentionPlane) -> Self {
		Self::for_class(engine, plane, RetentionClass::RowTtl)
	}

	fn for_class(engine: StandardEngine, plane: RetentionPlane, class: RetentionClass) -> Self {
		Self {
			evictor: Evictor::with_plane(engine, plane),
			state: EvictorState::default(),
			class,
		}
	}
}

impl LifecycleTask for RetentionEvictTask {
	fn name(&self) -> &'static str {
		match self.class {
			RetentionClass::RowTtl => "retention-evict-silent",
			_ => "retention-evict",
		}
	}

	fn interval(&self) -> Duration {
		self.evictor.engine.catalog().get_config_duration(ConfigKey::RetentionEvictInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		match self.class {
			RetentionClass::RowTtl => &[RetentionClass::RowTtl],
			_ => &[],
		}
	}

	#[instrument(name = "lifecycle::retention::evict::slice", level = "debug", skip_all)]
	fn run_slice(&mut self) -> Progress {
		let now = self.evictor.engine.clock().now();
		self.evictor.run_tick(&mut self.state, self.class, now)
	}
}

#[cfg(test)]
mod tests {
	use std::{
		thread::sleep,
		time::{Duration, Instant},
	};

	use reifydb_catalog::cache::{CatalogCache, load::CatalogCacheLoader};
	use reifydb_cdc::{produce::watermark::CdcProducerWatermark, storage::CdcStore};
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{
		common::CommitVersion,
		interface::{
			catalog::{
				ringbuffer::{
					PartitionedMetadata, RingBuffer, RingBufferMetadata, encode_ringbuffer_metadata,
				},
				series::SeriesMetadata,
			},
			store::MultiVersionRow,
		},
		key::ringbuffer::RingBufferMetadataKey,
	};
	use reifydb_runtime::version_epoch::EpochSpan;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::value::row_number::RowNumber;

	use super::*;

	const HOUR: EpochSpan = EpochSpan::new(3_600);

	const FOUR_HOURS: EpochSpan = EpochSpan::new(4 * 3_600);

	const HOUR_NANOS: i64 = 3_600 * 1_000_000_000;

	fn age_past_ttl(test: &TestEngine) {
		// Rows are stamped from the clock at write time, so advancing past the ttl expires
		// everything written before this call and nothing written after it.
		test.mock_clock().advance_secs(HOUR.seconds() + 1);
	}

	fn tick_now(test: &TestEngine, state: &mut EvictorState, class: RetentionClass) -> Progress {
		Evictor::new(test.inner().clone()).run_tick(state, class, test.mock_clock().now())
	}

	fn row_count(test: &TestEngine, rql: &str) -> usize {
		TestEngine::row_count(&test.query(rql))
	}

	fn ringbuffer_partitions(engine: &StandardEngine, name: &str) -> Vec<PartitionedMetadata> {
		let catalog = engine.catalog();
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let namespace =
			catalog.find_namespace_by_name(&mut Transaction::Command(&mut txn), "test").unwrap().unwrap();
		let ringbuffer = catalog
			.find_ringbuffer_by_name(&mut Transaction::Command(&mut txn), namespace.id(), name)
			.unwrap()
			.unwrap();
		let partitions =
			catalog.list_ringbuffer_partitions(&mut Transaction::Command(&mut txn), &ringbuffer).unwrap();
		txn.rollback().unwrap();
		partitions
	}

	fn series_metadata(engine: &StandardEngine, name: &str) -> SeriesMetadata {
		let catalog = engine.catalog();
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let namespace =
			catalog.find_namespace_by_name(&mut Transaction::Command(&mut txn), "test").unwrap().unwrap();
		let series = catalog
			.find_series_by_name(&mut Transaction::Command(&mut txn), namespace.id(), name)
			.unwrap()
			.unwrap();
		let metadata =
			catalog.find_series_metadata(&mut Transaction::Command(&mut txn), series.id).unwrap().unwrap();
		txn.rollback().unwrap();
		metadata
	}

	fn ringbuffer_by_name(engine: &StandardEngine, name: &str) -> RingBuffer {
		let catalog = engine.catalog();
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let namespace =
			catalog.find_namespace_by_name(&mut Transaction::Command(&mut txn), "test").unwrap().unwrap();
		let ringbuffer = catalog
			.find_ringbuffer_by_name(&mut Transaction::Command(&mut txn), namespace.id(), name)
			.unwrap()
			.unwrap();
		txn.rollback().unwrap();
		ringbuffer
	}

	fn underlying_ringbuffer(engine: &StandardEngine) -> RingBuffer {
		let catalog = engine.catalog();
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let namespace =
			catalog.find_namespace_by_name(&mut Transaction::Command(&mut txn), "test").unwrap().unwrap();
		let all = catalog.list_ringbuffers_all(&mut Transaction::Command(&mut txn)).unwrap();
		txn.rollback().unwrap();
		all.into_iter()
			.find(|rb| rb.underlying && rb.namespace == namespace.id())
			.expect("the deferred ringbuffer view must create an underlying ring buffer")
	}

	fn seed_partition(engine: &StandardEngine, id: RingBufferId, values: Vec<Value>) {
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let mut metadata = RingBufferMetadata::new();
		metadata.count = 1;
		metadata.tail = 2;
		txn.set(
			&RingBufferMetadataKey::encoded_partition(id, values),
			encode_ringbuffer_metadata(&metadata).into_bytes(),
		)
		.unwrap();
		txn.commit().unwrap();
	}

	fn catalog_partition_values(engine: &StandardEngine, ringbuffer: &RingBuffer) -> Vec<Vec<Value>> {
		let catalog = engine.catalog();
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let partitions =
			catalog.list_ringbuffer_partitions(&mut Transaction::Command(&mut txn), ringbuffer).unwrap();
		txn.rollback().unwrap();
		partitions.into_iter().map(|p| p.partition_values).collect()
	}

	fn ringbuffer_rows(engine: &StandardEngine, ringbuffer: &RingBuffer) -> Vec<MultiVersionRow> {
		let storage = StorageId::RingBuffer(ringbuffer.id);
		let keyspace = if ringbuffer.partition_by.is_empty() {
			RowKey::full_scan(storage)
		} else {
			PartitionedRowKey::full_scan(storage)
		};
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let rows: Vec<MultiVersionRow> =
			txn.range(keyspace, RangeScope::All, 1024).unwrap().map(|row| row.unwrap()).collect();
		txn.rollback().unwrap();
		rows
	}

	fn put_ringbuffer_row(
		engine: &StandardEngine,
		id: RingBufferId,
		partition_values: &[Value],
		row_number: RowNumber,
		bytes: EncodedBytes,
	) {
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		txn.set(
			&PartitionedRowKey::encoded(id, Partition::of(partition_values), RowLocator::Row(row_number)),
			bytes,
		)
		.unwrap();
		txn.commit().unwrap();
	}

	fn wait_cdc_watermark(engine: &StandardEngine, version: CommitVersion) {
		let watermark = engine.ioc().try_resolve::<CdcProducerWatermark>().unwrap();
		let deadline = Instant::now() + Duration::from_secs(5);
		while watermark.get() < version {
			assert!(
				Instant::now() < deadline,
				"the cdc producer did not reach version {version:?} within the deadline"
			);
			sleep(Duration::from_millis(10));
		}
	}

	#[test]
	fn two_rows_a_millisecond_apart_expire_independently() {
		// Expiry anchors on each row's own updated_at, not a quantised epoch sample, so a cutoff
		// falling between two writes 1ms apart must kill exactly one of them.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::t { v: int4 } with { time: processing, row: { ttl: 1h } }");

		test.command("INSERT test::t [{ v: 1 }]");
		let first_write = test.mock_clock().now();
		test.mock_clock().advance_millis(1);
		test.command("INSERT test::t [{ v: 2 }]");

		// The cutoff lands on the first write's own instant and expiry is inclusive, so v=1 dies
		// at the boundary and v=2 lives.
		let mut state = EvictorState::default();
		Evictor::new((*test).clone()).run_tick(
			&mut state,
			RetentionClass::RowTtl,
			first_write.checked_add(HOUR.to_duration()).unwrap(),
		);

		assert_eq!(
			row_count(&test, "from test::t"),
			1,
			"the ttl boundary falls between the two writes, so exactly one row may survive"
		);
		assert_eq!(
			row_count(&test, "from test::t filter v == 2"),
			1,
			"and it must be the younger one; evicting v=2 would mean the cutoff was rounded up"
		);
	}

	#[test]
	fn table_drop_mode_evicts_rows_silently_without_cdc() {
		// Row eviction must commit without emitting CDC, otherwise every expiry leaks downstream as a delete.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::t { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);

		let before = test.current_version().unwrap();
		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtl);
		let after = test.current_version().unwrap();

		assert_eq!(row_count(&test, "from test::t"), 0);
		assert!(after > before, "silent eviction must still be a real commit");

		wait_cdc_watermark(&test, after);
		let cdc = test.ioc().try_resolve::<CdcStore>().unwrap();
		for version in (before.0 + 1)..=after.0 {
			assert!(
				cdc.read(CommitVersion(version)).unwrap().is_none(),
				"silent eviction of a plain table must not write any CDC record"
			);
		}
	}

	#[test]
	fn partitioned_ringbuffer_delete_mode_maintains_partition_metadata() {
		// Partition metadata must move in the same commit as the row removals: a fully expired
		// partition loses its metadata key, a partial one has count decremented and head advanced.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::rb { a: utf8, v: int4 } WITH { time: processing, capacity: 100, row: { ttl: 1h }, partition: { by: { a } } }",
		);
		test.command("INSERT test::rb [{ a: \"us\", v: 1 }, { a: \"us\", v: 2 }, { a: \"us\", v: 3 }]");
		test.command("INSERT test::rb [{ a: \"eu\", v: 10 }, { a: \"eu\", v: 20 }]");
		age_past_ttl(&test);
		test.command("INSERT test::rb [{ a: \"eu\", v: 30 }]");

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(row_count(&test, "from test::rb"), 1, "only the eu row inserted after the epoch survives");

		let partitions = ringbuffer_partitions(&test, "rb");
		assert_eq!(
			partitions.len(),
			1,
			"the fully expired us partition must lose its metadata key, not keep a zero-count entry"
		);
		let eu = &partitions[0];
		assert_eq!(eu.metadata.count, 1);
		assert_eq!(
			eu.metadata.head,
			eu.metadata.tail - 1,
			"head must advance to the single surviving row in the same commit"
		);
	}

	#[test]
	fn plain_ringbuffer_delete_mode_evicts_and_removes_empty_metadata() {
		// A non-partitioned ring buffer must get the same metadata maintenance on its
		// whole-buffer entry as a partitioned one gets per partition.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::rb { v: int4 } WITH { time: processing, capacity: 100, row: { ttl: 1h } }",
		);
		test.command("INSERT test::rb [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);
		test.command("INSERT test::rb [{ v: 3 }]");

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(row_count(&test, "from test::rb"), 1);
		let partitions = ringbuffer_partitions(&test, "rb");
		assert_eq!(partitions.len(), 1);
		assert_eq!(partitions[0].metadata.count, 1);

		age_past_ttl(&test);
		tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(row_count(&test, "from test::rb"), 0);
		assert!(
			ringbuffer_partitions(&test, "rb").is_empty(),
			"a fully drained buffer must not leak a zero-count metadata entry"
		);
	}

	#[test]
	fn plain_ringbuffer_drop_mode_evicts_and_maintains_metadata() {
		// Silence applies to CDC, not to bookkeeping: count and head must still move in the same
		// commit or later inserts and evictions desync.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::rb { v: int4 } WITH { time: processing, capacity: 100, row: { ttl: 1h } }",
		);
		test.command("INSERT test::rb [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);
		test.command("INSERT test::rb [{ v: 3 }]");

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(row_count(&test, "from test::rb"), 1);
		let partitions = ringbuffer_partitions(&test, "rb");
		assert_eq!(partitions.len(), 1);
		assert_eq!(partitions[0].metadata.count, 1);
		assert_eq!(partitions[0].metadata.head, partitions[0].metadata.tail - 1);
	}

	#[test]
	fn budget_bounds_the_tick_and_cursor_resumes_on_the_next() {
		// A tick is capped at batch_size x max_batches rows; the leftover must resume from the
		// cursor rather than be lost or drained in one unbounded tick.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::t { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(2));
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }, { v: 5 }]");
		age_past_ttl(&test);

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtl);
		assert_eq!(
			row_count(&test, "from test::t"),
			1,
			"tick one is capped at 2 batches x 2 rows; one expired row must be left over"
		);

		tick_now(&test, &mut state, RetentionClass::RowTtl);
		assert_eq!(row_count(&test, "from test::t"), 0, "the cursor must resume and drain the leftover");
	}

	#[test]
	fn a_cutoff_the_clock_cannot_place_evicts_nothing() {
		// When `now - ttl` underflows there is no cutoff to apply, and the evictor must delete
		// nothing: a permissive fallback would expire every row whose age it cannot establish.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::t { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }]");

		let now = DateTime::from_nanos(HOUR.seconds() * 1_000_000_000 / 2);
		assert!(
			now.checked_sub(HOUR.to_duration()).is_none(),
			"precondition: the cutoff must be unresolvable, or this asserts nothing"
		);

		let mut state = EvictorState::default();
		Evictor::new((*test).clone()).run_tick(&mut state, RetentionClass::RowTtl, now);

		assert_eq!(row_count(&test, "from test::t"), 2, "nothing may be evicted on a guess about age");
	}

	#[test]
	fn series_delete_mode_evicts_rows_and_decrements_row_count() {
		// row_count must be decremented in the same commit as the removals, so no reader can
		// observe rows gone but still counted.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create series test::s { ts: datetime, v: int4 } WITH { time: processing, key: ts, row: { ttl: 1h } }",
		);
		test.command(
			"INSERT test::s [{ ts: datetime::from_epoch_millis(1000), v: 1 }, { ts: datetime::from_epoch_millis(2000), v: 2 }]",
		);
		age_past_ttl(&test);
		test.command("INSERT test::s [{ ts: datetime::from_epoch_millis(3000), v: 3 }]");
		assert_eq!(series_metadata(&test, "s").row_count, 3);

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(row_count(&test, "from test::s"), 1);
		assert_eq!(
			series_metadata(&test, "s").row_count,
			1,
			"row_count must be decremented in the same commit as the row removals"
		);
	}

	#[test]
	fn dml_delete_and_evictor_produce_identical_ringbuffer_metadata() {
		// DML DELETE and the evictor share one metadata helper, so the same removals from the
		// same starting state must land on identical metadata; divergence means it forked.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::dml { a: utf8, v: int4 } WITH { capacity: 100, partition: { by: { a } } }",
		);
		test.admin(
			"CREATE RINGBUFFER test::evicted { a: utf8, v: int4 } WITH { time: processing, capacity: 100, row: { ttl: 1h }, partition: { by: { a } } }",
		);
		for rql in [
			"INSERT test::dml [{ a: \"us\", v: 0 }, { a: \"us\", v: 1 }, { a: \"us\", v: 2 }]",
			"INSERT test::evicted [{ a: \"us\", v: 0 }, { a: \"us\", v: 1 }, { a: \"us\", v: 2 }]",
		] {
			test.command(rql);
		}
		age_past_ttl(&test);
		for rql in [
			"INSERT test::dml [{ a: \"us\", v: 3 }, { a: \"us\", v: 4 }]",
			"INSERT test::evicted [{ a: \"us\", v: 3 }, { a: \"us\", v: 4 }]",
		] {
			test.command(rql);
		}

		test.command("DELETE test::dml FILTER v < 3");
		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(row_count(&test, "from test::dml"), 2);
		assert_eq!(row_count(&test, "from test::evicted"), 2);

		let dml = ringbuffer_partitions(&test, "dml");
		let evicted = ringbuffer_partitions(&test, "evicted");
		assert_eq!(dml.len(), 1);
		assert_eq!(evicted.len(), 1);
		assert_eq!(
			(dml[0].metadata.count, dml[0].metadata.head, dml[0].metadata.tail),
			(evicted[0].metadata.count, evicted[0].metadata.head, evicted[0].metadata.tail),
			"DML DELETE and the evictor must produce identical partition metadata from the same state"
		);
	}

	#[test]
	fn underlying_ring_buffers_are_skipped_they_are_owned_by_the_sink_operator() {
		// A view-backed ring buffer is evicted by its sink operator, which owns the per-partition
		// state; a second reaper would strand that state and bypass the operator's downstream
		// eviction propagation. A standalone ring buffer stays evictor-owned.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::src { base: utf8, n: int4 }");
		// No flow subsystem runs here, but the DDL still creates the backing ring buffer and
		// registers its row ttl.
		test.admin(
			"create deferred ringbuffer view test::rb { base: utf8, n: int4 } WITH { capacity: 100, row: { ttl: 1h }, partition: { by: { base } } } as { from test::src }",
		);
		test.admin(
			"CREATE RINGBUFFER test::standalone { base: utf8, n: int4 } WITH { time: processing, capacity: 100, row: { ttl: 1h }, partition: { by: { base } } }",
		);
		test.command("INSERT test::standalone [{ base: \"us\", n: 1 }]");

		let underlying = underlying_ringbuffer(&test);
		let standalone = ringbuffer_by_name(&test, "standalone");
		let us = vec![Value::Utf8("us".to_string())];

		// Without a real stamped row the skip assertion below passes on an empty keyspace.
		let seeded = ringbuffer_rows(&test, &standalone);
		assert_eq!(seeded.len(), 1, "precondition: the standalone insert must have produced exactly one row");
		put_ringbuffer_row(&test, underlying.id, &us, RowNumber(1), seeded[0].bytes.clone());
		seed_partition(&test, underlying.id, us.clone());

		assert_eq!(
			catalog_partition_values(&test, &underlying),
			vec![us.clone()],
			"the catalog must hold the seeded partition metadata (guards against a vacuous skip test)"
		);

		age_past_ttl(&test);
		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert!(
			ringbuffer_rows(&test, &standalone).is_empty(),
			"a standalone ring buffer must remain owned by the retention evictor"
		);
		assert_eq!(
			ringbuffer_rows(&test, &underlying).len(),
			1,
			"the retention evictor must skip underlying (view-backed) ring buffers"
		);
		assert_eq!(
			catalog_partition_values(&test, &underlying),
			vec![us],
			"skipping must leave the underlying buffer's partition metadata untouched"
		);
	}

	#[test]
	fn budget_exhausted_with_a_live_cursor_yields_for_catchup() {
		// A budget spent with rows still behind a live cursor must report Yielded, or the backlog
		// waits a full eviction interval instead of the lane's catch-up tick.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::t { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(2));
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }, { v: 5 }, { v: 6 }]");
		age_past_ttl(&test);

		let mut state = EvictorState::default();
		let progress = tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(
			progress,
			Progress::Yielded,
			"budget exhausted with a live cursor must yield so the catch-up tick drains the backlog"
		);
		assert_eq!(row_count(&test, "from test::t"), 2, "the tick is capped at 2 batches x 2 rows");

		let drained = tick_now(&test, &mut state, RetentionClass::RowTtl);
		assert_eq!(
			drained,
			Progress::Exhausted,
			"once the backlog is gone the same slice must report Exhausted"
		);
		assert_eq!(row_count(&test, "from test::t"), 0);
	}

	#[test]
	fn budget_exhausted_at_an_object_boundary_yields_on_unvisited_work() {
		// Backlog must count objects the budget never reached, not only ones left mid-scan: the
		// first table drains its cursor cleanly and the second is never visited at all.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::t1 { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.admin("create table test::t2 { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(1));
		test.command("INSERT test::t1 [{ v: 1 }, { v: 2 }]");
		test.command("INSERT test::t2 [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);

		let mut state = EvictorState::default();
		let progress = tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(
			progress,
			Progress::Yielded,
			"a budget spent exactly at an object boundary must still yield for the unvisited table"
		);
		assert_eq!(
			row_count(&test, "from test::t1") + row_count(&test, "from test::t2"),
			2,
			"exactly one table's two rows were evicted this tick; the other is untouched backlog"
		);
	}

	#[test]
	fn a_pass_over_objects_with_nothing_to_evict_reports_exhausted() {
		// Re-confirming an object has nothing expired must not charge budget, or a tree larger
		// than the budget never finishes a pass and the lane respins forever reclaiming nothing.
		// The clock advances before the inserts so every row sits above the cutoff.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		for name in ["t1", "t2", "t3", "t4"] {
			test.admin(&format!(
				"create table test::{name} {{ v: int4 }} with {{ time: processing, row: {{ ttl: 1h }} }}"
			));
		}
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(2));
		age_past_ttl(&test);
		for name in ["t1", "t2", "t3", "t4"] {
			test.command(&format!("INSERT test::{name} [{{ v: 1 }}, {{ v: 2 }}]"));
		}

		let mut state = EvictorState::default();
		let progress = tick_now(&test, &mut state, RetentionClass::RowTtl);

		assert_eq!(
			progress,
			Progress::Exhausted,
			"a full pass that found nothing to evict must report Exhausted so the lane sleeps for \
			 the eviction interval instead of respinning every 5ms"
		);
		for name in ["t1", "t2", "t3", "t4"] {
			assert_eq!(
				row_count(&test, &format!("from test::{name}")),
				2,
				"{name} holds only rows committed above the cutoff; none may be evicted"
			);
		}
	}

	#[test]
	fn every_object_is_visited_across_ticks_when_backlog_exceeds_the_budget() {
		// A tick must resume where the previous one stopped, or the budget is spent on the same
		// leading objects every time and everything behind them starves - expired rows never
		// reclaimed, which presents as an unbounded memory leak rather than a retention bug.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		for name in ["t1", "t2", "t3", "t4"] {
			test.admin(&format!(
				"create table test::{name} {{ v: int4 }} with {{ time: processing, row: {{ ttl: 1h }} }}"
			));
			test.command(&format!(
				"INSERT test::{name} [{{ v: 1 }}, {{ v: 2 }}, {{ v: 3 }}, {{ v: 4 }}, {{ v: 5 }}, {{ v: 6 }}]"
			));
		}
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(2));
		age_past_ttl(&test);

		let mut state = EvictorState::default();
		for _ in 0..4 {
			tick_now(&test, &mut state, RetentionClass::RowTtl);
		}

		for name in ["t1", "t2", "t3", "t4"] {
			let remaining = row_count(&test, &format!("from test::{name}"));
			assert!(
				remaining < 6,
				"{name} still holds all 6 expired rows after four ticks; the evictor never \
				 advanced past the head of the object list"
			);
		}
	}

	#[test]
	fn the_reported_floor_spans_every_eligible_object_not_only_the_visited_ones() {
		// The floor a class reports is what bounds everything that class may delete, so it has to be
		// the most conservative cutoff across every object the class owns. Folded over only the objects
		// a budgeted tick reached, the reported floor swings with the resume rotation instead, and a
		// floor that swings is read downstream as a floor that will not advance.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::short { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.admin("create table test::long { v: int4 } with { time: processing, row: { ttl: 4h } }");
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(1));
		// The clock starts at the epoch, where `now - 4h` underflows and the 4h object would resolve no
		// cutoff at all; it has to sit past the longest declared ttl for either object to have a floor.
		test.mock_clock().advance_secs(FOUR_HOURS.seconds() + HOUR.seconds());
		test.command("INSERT test::short [{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }]");
		test.command("INSERT test::long [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);

		let evictor = Evictor::new(test.inner().clone());
		let now = test.mock_clock().now();
		let mut state = EvictorState::default();
		let progress = evictor.run_tick(&mut state, RetentionClass::RowTtl, now);

		assert_eq!(
			progress,
			Progress::Yielded,
			"precondition: the budget must run out before the tick ever reaches test::long"
		);
		assert_eq!(
			evictor.plane().snapshot(RetentionClass::RowTtl).floor_version,
			now.checked_sub(FOUR_HOURS.to_duration()).unwrap().to_nanos(),
			"the floor must come from the 4h object this tick never visited, because that is the \
			 oldest cutoff the class is still bound by"
		);
	}

	#[test]
	fn the_reported_floor_does_not_regress_when_the_rotation_reaches_a_longer_ttl() {
		// Consecutive ticks visit different objects because an exhausted budget rotates the starting
		// offset. A floor folded over the visited subset drops by the whole ttl spread the moment the
		// rotation lands on a longer-lived object, and a floor that moves backwards while no work is
		// done is precisely the state the plane reports as eligible work behind a floor that will not
		// advance.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::short { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.admin("create table test::long { v: int4 } with { time: processing, row: { ttl: 4h } }");
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(1));
		// The clock starts at the epoch, where `now - 4h` underflows and the 4h object would resolve no
		// cutoff at all; it has to sit past the longest declared ttl for either object to have a floor.
		test.mock_clock().advance_secs(FOUR_HOURS.seconds() + HOUR.seconds());
		test.command("INSERT test::short [{ v: 1 }, { v: 2 }]");
		test.command("INSERT test::long [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);

		let evictor = Evictor::new(test.inner().clone());
		let mut state = EvictorState::default();
		evictor.run_tick(&mut state, RetentionClass::RowTtl, test.mock_clock().now());

		assert_eq!(
			row_count(&test, "from test::short"),
			0,
			"precondition: the first tick must spend its whole budget on the 1h object"
		);
		let first = evictor.plane().snapshot(RetentionClass::RowTtl).floor_version;

		test.mock_clock().advance_secs(600);
		let second = test.mock_clock().now();
		evictor.run_tick(&mut state, RetentionClass::RowTtl, second);

		let snapshot = evictor.plane().snapshot(RetentionClass::RowTtl);

		assert_eq!(
			snapshot.floor_version,
			second.checked_sub(FOUR_HOURS.to_duration()).unwrap().to_nanos(),
			"the second tick starts at the 4h object, and the class floor must still be measured from \
			 the longest declared ttl rather than from whatever the rotation happened to reach"
		);
		assert!(
			snapshot.floor_version > first,
			"a floor derived from the clock and a fixed set of ttls can only move forwards"
		);
		assert_eq!(
			snapshot.stuck_slices, 0,
			"a class whose floor advances on every tick must never be accounted as stuck"
		);
	}

	#[test]
	fn unresolvable_floor_returns_exhausted_not_yielded() {
		// An unresolvable cutoff means the class has no eligible work: yielding would spin the
		// lane at the catch-up cadence and starve the other classes.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::t { v: int4 } with { time: processing, row: { ttl: 1h } }");
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }]");

		let now = DateTime::from_nanos(HOUR.seconds() * 1_000_000_000 / 2);
		assert!(
			now.checked_sub(HOUR.to_duration()).is_none(),
			"precondition: the cutoff must be unresolvable, or this asserts nothing"
		);

		let mut state = EvictorState::default();
		let progress = Evictor::new((*test).clone()).run_tick(&mut state, RetentionClass::RowTtl, now);

		assert_eq!(progress, Progress::Exhausted, "an unresolvable floor must not spin the lane");
		assert_eq!(row_count(&test, "from test::t"), 2, "and nothing may be evicted on a guess about age");
	}

	fn registered_row_ttls(engine: &StandardEngine) -> Vec<(i64, bool)> {
		engine.catalog()
			.list_row_settings()
			.into_iter()
			.filter_map(|(_, settings)| {
				let ttl = settings.ttl?;
				Some((ttl.duration.as_nanos().ok()?, settings.persistent))
			})
			.collect()
	}

	fn object_with_row_ttl(engine: &StandardEngine, nanos: i64) -> StorageId {
		engine.catalog()
			.list_row_settings()
			.into_iter()
			.find(|(_, settings)| {
				settings.ttl.as_ref().and_then(|ttl| ttl.duration.as_nanos().ok()) == Some(nanos)
			})
			.map(|(object, _)| object)
			.expect("the declared row ttl must be registered before it can be rehydrated")
	}

	#[test]
	fn a_deferred_view_registers_its_row_ttl_whether_or_not_it_declares_persistence() {
		// The work list reads the catalog cache with no storage fallback and no warning, so an
		// object missing from it keeps its rows forever behind a ttl its own DDL advertises.
		// Presence must hold independent of the `persistent` flag, which once gated it.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::src { base: utf8, n: int4 }");
		test.admin(
			"create deferred view test::implicit { base: utf8, n: int4 } with { row: { ttl: 1h } } as { from test::src }",
		);
		test.admin(
			"create deferred view test::explicit { base: utf8, n: int4 } with { row: { ttl: 2h, persistent: false } } as { from test::src }",
		);

		let registered = registered_row_ttls(&test);

		assert!(
			registered.contains(&(HOUR_NANOS, true)),
			"the view declaring only `row: {{ ttl }}` must register a persistent row ttl, got {registered:?}"
		);
		assert!(
			registered.contains(&(2 * HOUR_NANOS, false)),
			"the view declaring `persistent: false` must register too, got {registered:?}"
		);
	}

	#[test]
	fn a_cold_catalog_cache_recovers_a_declared_row_ttl_from_storage() {
		// A row-settings entry that reaches storage but not the rehydrated cache leaves the object
		// silently perpetual for the life of the process. A fresh cache over the same store is
		// that restart, minus the disk.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::src { base: utf8, n: int4 }");
		test.admin(
			"create deferred view test::implicit { base: utf8, n: int4 } with { row: { ttl: 1h } } as { from test::src }",
		);

		let object = object_with_row_ttl(&test, HOUR_NANOS);

		let cold = CatalogCache::new();
		let mut txn = test.begin_command(IdentityId::system()).unwrap();
		CatalogCacheLoader::load_all(&mut Transaction::Command(&mut txn), &cold).unwrap();
		txn.rollback().unwrap();

		let recovered = cold
			.find_row_settings(object)
			.expect("hydration dropped the row settings, so the object is perpetual after a restart");

		assert_eq!(
			recovered.ttl.and_then(|ttl| ttl.duration.as_nanos().ok()),
			Some(HOUR_NANOS),
			"the rehydrated ttl must match the declared one, or expiry silently changes across a restart"
		);
		assert!(recovered.persistent, "a view that declares no persistence flag rehydrates as persistent");
	}
}
