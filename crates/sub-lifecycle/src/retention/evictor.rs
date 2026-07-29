// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	event::row::RowsExpiredEvent,
	interface::{
		WithEventBus,
		catalog::{
			config::{ConfigKey, GetConfig},
			id::{RingBufferId, SeriesId, TableId},
			storage::StorageId,
		},
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
		ringbuffer::{RingBufferOperations, apply_ringbuffer_partition_metadata_after_delete},
		series::{
			apply_series_metadata_after_delete, build_series_delete_pre_columns_from_storage,
			decode_series_storage_key, remove_series_row,
		},
		table::TableOperations,
	},
	vm::instruction::dml::shape::get_or_create_series_shape,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	value::{
		Value, datetime::DateTime, duration::Duration, identity::IdentityId, partition::Partition,
		row_number::RowNumber,
	},
};
use tracing::{debug, instrument, warn};

use crate::{plane::RetentionPlane, retention::scan};

type CursorKey = (StorageId, EncodedKey);

#[derive(Default)]
pub struct EvictorState {
	running: bool,
	cursors: HashMap<CursorKey, EncodedKey>,
	resume: Option<StorageId>,
}

#[derive(Default)]
struct ClassTally {
	floor: Option<(Floor, FloorTerm)>,
	rows: u64,
	backlog: u64,
	resolved_any: bool,
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
}

impl Evictor {
	pub fn new(engine: StandardEngine) -> Self {
		let plane = RetentionPlane::for_engine(&engine, RetentionMetrics::new());
		Self::with_plane(engine, plane)
	}

	pub fn with_plane(engine: StandardEngine, plane: RetentionPlane) -> Self {
		Self {
			engine,
			plane,
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
				(Self::class_of(ttl.announce) == target).then_some((storage, ttl))
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
			let Some((cutoff, binding)) = self.expiry_cutoff(now, ttl) else {
				stats.objects_skipped += 1;
				resume_after = Some(storage);
				continue;
			};
			tally.resolved_any = true;
			tally.floor = Some(match tally.floor {
				Some(held) if held.0.monotonic_key() <= cutoff.raw() => held,
				_ => (Floor::Instant(cutoff.instant()), binding),
			});

			stats.objects_scanned += 1;
			let expired_before = stats.rows_expired;
			if let Err(e) =
				self.evict_storage(state, storage, ttl, cutoff, batch_size, &mut budget, &mut stats)
			{
				warn!(?storage, error = %e, "retention eviction failed; resetting cursors, retrying next tick");
				state.cursors.retain(|key, _| key.0 != storage);
				budget = budget.saturating_sub(1);
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

		let floor = if tally.resolved_any {
			tally.floor
		} else {
			None
		};
		let backlog = tally.backlog + u64::from(unvisited_eligible);
		self.plane.record_reclamation(target, floor, tally.rows, backlog);
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

	fn expiry_cutoff(&self, now: DateTime, ttl: &Ttl) -> Option<(Cutoff, FloorTerm)> {
		let (floor, binding) =
			self.plane.cutoff_with_binding(Self::class_of(ttl.announce), now, Some(ttl.duration))?;
		Some((Cutoff(floor.instant()?), binding))
	}

	fn class_of(announce: bool) -> RetentionClass {
		if announce {
			RetentionClass::RowTtlAnnounced
		} else {
			RetentionClass::RowTtlSilent
		}
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "lifecycle::retention::evict::object", level = "debug", skip_all)]
	fn evict_storage(
		&self,
		state: &mut EvictorState,
		storage: StorageId,
		ttl: &Ttl,
		cutoff: Cutoff,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		match storage {
			StorageId::Table(id) => {
				self.evict_table(state, id, ttl.announce, cutoff, batch_size, budget, stats)
			}
			StorageId::RingBuffer(id) => {
				self.evict_ringbuffer(state, id, ttl.announce, cutoff, batch_size, budget, stats)
			}
			StorageId::Series(id) => {
				self.evict_series(state, id, ttl.announce, cutoff, batch_size, budget, stats)
			}
			StorageId::Queue(_) => Ok(()),
		}
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "lifecycle::retention::evict::table", level = "debug", skip_all)]
	fn evict_table(
		&self,
		state: &mut EvictorState,
		id: TableId,
		announce: bool,
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
					self.evict_table_batch(state, id, announce, cutoff, batch_size, &keyspace)?;
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

	#[instrument(name = "lifecycle::retention::evict::table_batch", level = "trace", skip_all)]
	fn evict_table_batch(
		&self,
		state: &mut EvictorState,
		id: TableId,
		announce: bool,
		cutoff: Cutoff,
		batch_size: usize,
		keyspace: &EncodedKeyRange,
	) -> Result<(u64, bool)> {
		let storage = StorageId::Table(id);
		let cursor_key = (storage, scan::keyspace_start(keyspace));
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		let Some(table) = catalog.find_table(&mut Transaction::Command(&mut txn), id)? else {
			txn.rollback()?;
			state.cursors.retain(|key, _| key.0 != storage);
			return Ok((0, true));
		};

		let range = scan::resume_range(keyspace, state.cursors.get(&cursor_key));
		let result = scan::scan_expired(&mut txn, range, cutoff, batch_size, &|_| None)?;
		if result.expired.is_empty() {
			txn.rollback()?;
			return Ok((0, advance_cursor(state, cursor_key, result.next_cursor)));
		}

		let rows = result.expired.len() as u64;
		match announce {
			true => {
				let mut ids: Vec<RowNumber> = Vec::with_capacity(result.expired.len());
				let mut partitions: Vec<Partition> = Vec::new();
				for row in &result.expired {
					let Some((row_number, partition)) = decode_table_locator(&row.key) else {
						continue;
					};
					ids.push(row_number);
					if let Some(partition) = partition {
						partitions.push(partition);
					}
				}
				assert!(
					partitions.is_empty() || partitions.len() == ids.len(),
					"table eviction batch mixed partitioned and plain row keys"
				);
				txn.remove_from_table(&table, &ids, &partitions)?;
			}
			false => {
				for row in &result.expired {
					txn.remove_silent(&row.key)?;
				}
			}
		}
		txn.commit()?;
		Ok((rows, advance_cursor(state, cursor_key, result.next_cursor)))
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "lifecycle::retention::evict::ringbuffer", level = "debug", skip_all)]
	fn evict_ringbuffer(
		&self,
		state: &mut EvictorState,
		id: RingBufferId,
		announce: bool,
		cutoff: Cutoff,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		for partition_values in self.list_ringbuffer_partitions(id)? {
			loop {
				if *budget == 0 {
					return Ok(());
				}
				let (rows, drained) = self.evict_ringbuffer_partition_batch(
					state,
					id,
					announce,
					cutoff,
					batch_size,
					&partition_values,
				)?;
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

	fn list_ringbuffer_partitions(&self, id: RingBufferId) -> Result<Vec<Vec<Value>>> {
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;
		let result = (|| {
			let Some(ringbuffer) = catalog.find_ringbuffer(&mut Transaction::Command(&mut txn), id)? else {
				return Ok(Vec::new());
			};
			if ringbuffer.underlying {
				return Ok(Vec::new());
			}
			let partitions =
				catalog.list_ringbuffer_partitions(&mut Transaction::Command(&mut txn), &ringbuffer)?;
			Ok(partitions.into_iter().map(|p| p.partition_values).collect())
		})();
		match &result {
			Ok(_) => txn.rollback()?,
			Err(_) => {
				let _ = txn.rollback();
			}
		}
		result
	}

	#[instrument(name = "lifecycle::retention::evict::ringbuffer_batch", level = "trace", skip_all)]
	fn evict_ringbuffer_partition_batch(
		&self,
		state: &mut EvictorState,
		id: RingBufferId,
		announce: bool,
		cutoff: Cutoff,
		batch_size: usize,
		partition_values: &[Value],
	) -> Result<(u64, bool)> {
		let storage = StorageId::RingBuffer(id);
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		let Some(ringbuffer) = catalog.find_ringbuffer(&mut Transaction::Command(&mut txn), id)? else {
			txn.rollback()?;
			state.cursors.retain(|key, _| key.0 != storage);
			return Ok((0, true));
		};

		let partition = if ringbuffer.partition_by.is_empty() {
			None
		} else {
			Some(Partition::of(partition_values))
		};
		let keyspace = match partition {
			Some(partition) => PartitionedRowKey::partition_range(storage, partition),
			None => RowKey::full_scan(storage),
		};
		let cursor_key = (storage, scan::keyspace_start(&keyspace));

		let Some(metadata) = catalog.find_partition_metadata(
			&mut Transaction::Command(&mut txn),
			&ringbuffer,
			partition_values,
		)?
		else {
			txn.rollback()?;
			state.cursors.remove(&cursor_key);
			return Ok((0, true));
		};

		let partitioned = partition.is_some();
		let range = scan::resume_range(&keyspace, state.cursors.get(&cursor_key));
		let result = scan::scan_expired(&mut txn, range, cutoff, batch_size, &|key| {
			decode_ringbuffer_row_number(key, partitioned)
		})?;
		if result.expired.is_empty() {
			txn.rollback()?;
			return Ok((0, advance_cursor(state, cursor_key, result.next_cursor)));
		}

		let deleted = result.expired.len() as u64;
		match announce {
			true => {
				for row in &result.expired {
					let Some(row_number) = decode_ringbuffer_row_number(&row.key, partitioned)
					else {
						continue;
					};
					txn.remove_from_ringbuffer(&ringbuffer, partition, RowNumber(row_number))?;
				}
			}
			false => {
				for row in &result.expired {
					txn.remove_silent(&row.key)?;
				}
			}
		}
		apply_ringbuffer_partition_metadata_after_delete(
			&catalog,
			&mut Transaction::Command(&mut txn),
			&ringbuffer,
			partition_values,
			metadata,
			deleted,
			result.min_survivor_row,
		)?;
		txn.commit()?;
		Ok((deleted, advance_cursor(state, cursor_key, result.next_cursor)))
	}

	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "lifecycle::retention::evict::series", level = "debug", skip_all)]
	fn evict_series(
		&self,
		state: &mut EvictorState,
		id: SeriesId,
		announce: bool,
		cutoff: Cutoff,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		loop {
			if *budget == 0 {
				return Ok(());
			}
			let (rows, drained) = self.evict_series_batch(state, id, announce, cutoff, batch_size)?;
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
		announce: bool,
		cutoff: Cutoff,
		batch_size: usize,
	) -> Result<(u64, bool)> {
		let storage = StorageId::Series(id);
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		let Some(series) = catalog.find_series(&mut Transaction::Command(&mut txn), id)? else {
			txn.rollback()?;
			state.cursors.retain(|key, _| key.0 != storage);
			return Ok((0, true));
		};
		let Some(mut metadata) =
			catalog.find_series_metadata(&mut Transaction::Command(&mut txn), series.id)?
		else {
			txn.rollback()?;
			state.cursors.retain(|key, _| key.0 != storage);
			return Ok((0, true));
		};

		let partitioned = !series.partition_by.is_empty();
		let keyspace = if partitioned {
			PartitionedRowKey::full_scan(storage)
		} else {
			SeriesRowKeyRange::full_scan(series.id, None)
		};
		let cursor_key = (storage, scan::keyspace_start(&keyspace));

		let range = scan::resume_range(&keyspace, state.cursors.get(&cursor_key));
		let result = scan::scan_expired(&mut txn, range, cutoff, batch_size, &|_| None)?;
		if result.expired.is_empty() {
			txn.rollback()?;
			return Ok((0, advance_cursor(state, cursor_key, result.next_cursor)));
		}

		let deleted = result.expired.len() as u64;
		match announce {
			true => {
				let row_shape = get_or_create_series_shape(
					&catalog,
					&series,
					&mut Transaction::Command(&mut txn),
				)?;
				for row in &result.expired {
					let committed = txn.get_committed(&row.key)?.map(|v| v.row);
					let pre_for_cdc = committed.clone().unwrap_or_else(|| row.row.clone());
					let pre = decode_series_storage_key(&series, &row.key, partitioned).map(
						|decoded| {
							build_series_delete_pre_columns_from_storage(
								&series,
								&row_shape,
								&pre_for_cdc,
								&decoded,
							)
						},
					);
					remove_series_row(
						&mut Transaction::Command(&mut txn),
						&series,
						&row.key,
						pre_for_cdc,
						committed.is_some(),
						pre,
					)?;
				}
			}
			false => {
				for row in &result.expired {
					txn.remove_silent(&row.key)?;
				}
			}
		}
		apply_series_metadata_after_delete(&mut metadata, deleted);
		catalog.update_series_metadata_txn(&mut Transaction::Command(&mut txn), metadata)?;
		txn.commit()?;
		Ok((deleted, advance_cursor(state, cursor_key, result.next_cursor)))
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

fn decode_table_locator(key: &EncodedKey) -> Option<(RowNumber, Option<Partition>)> {
	if let Some(row_key) = RowKey::decode(key) {
		return Some((row_key.row, None));
	}
	let partitioned = PartitionedRowKey::decode(key)?;
	match partitioned.locator {
		RowLocator::Row(row_number) => Some((row_number, Some(partitioned.partition))),
		_ => None,
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
		Self::for_class(engine, plane, RetentionClass::RowTtlSilent)
	}

	pub fn announced(engine: StandardEngine, plane: RetentionPlane) -> Self {
		Self::for_class(engine, plane, RetentionClass::RowTtlAnnounced)
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
			RetentionClass::RowTtlSilent => "retention-evict-silent",
			RetentionClass::RowTtlAnnounced => "retention-evict-announced",
			_ => "retention-evict",
		}
	}

	fn interval(&self) -> Duration {
		self.evictor.engine.catalog().get_config_duration(ConfigKey::RetentionEvictInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		match self.class {
			RetentionClass::RowTtlSilent => &[RetentionClass::RowTtlSilent],
			RetentionClass::RowTtlAnnounced => &[RetentionClass::RowTtlAnnounced],
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
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			ringbuffer::{PartitionedMetadata, RingBuffer, RingBufferMetadata, encode_ringbuffer_metadata},
			series::SeriesMetadata,
		},
		key::ringbuffer::RingBufferMetadataKey,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::version_epoch::EpochSpan;

	use super::*;

	const HOUR: EpochSpan = EpochSpan::new(3_600);

	const HOUR_NANOS: i64 = 3_600 * 1_000_000_000;

	// Expiry is time-anchored: a row is expired iff its own `updated_at` is at or below
	// `now - ttl`. Rows are stamped from the clock when they are written, so `age_past_ttl`
	// pushes everything written before it out of the ttl window and leaves everything written
	// after it inside. `tick_now` then evicts at the clock's current instant.
	fn age_past_ttl(test: &TestEngine) {
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
		let mut metadata = RingBufferMetadata::new(id, 100);
		metadata.count = 1;
		metadata.tail = 2;
		txn.set(&RingBufferMetadataKey::encoded_partition(id, values), encode_ringbuffer_metadata(&metadata))
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

	fn evictor_partition_values(engine: &StandardEngine, id: RingBufferId) -> Vec<Vec<Value>> {
		Evictor::new(engine.clone()).list_ringbuffer_partitions(id).unwrap()
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
		// The point of anchoring expiry to the row's own updated_at rather than to a version epoch
		// sample. The epoch quantised time to whole-second samples, so any two rows landing in the
		// same sample shared a fate no matter how far apart the ttl boundary actually fell between
		// them - which is why the ttl floor had to be a whole second. Here the cutoff lands between
		// two rows written 1ms apart, and exactly one of them dies.
		//
		// Mutation: round the cutoff or the stamp to seconds and both rows go, or neither.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);

		test.command("INSERT test::t [{ v: 1 }]");
		let first_write = test.mock_clock().now();
		test.mock_clock().advance_millis(1);
		test.command("INSERT test::t [{ v: 2 }]");

		// Ticking exactly one ttl after the first write puts the cutoff on that write's own
		// instant. Expiry is inclusive, so v=1 is at the boundary and dies; v=2, one millisecond
		// younger, is past it and lives.
		let mut state = EvictorState::default();
		Evictor::new((*test).clone()).run_tick(
			&mut state,
			RetentionClass::RowTtlAnnounced,
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
	fn table_delete_mode_evicts_expired_rows_transactionally() {
		// An announced TTL must run through the engine operation helpers, so the eviction is a
		// real commit: rows disappear for readers and the commit produces a CDC record
		// (unlike the retired gc/row path, which bypassed the pipeline entirely).
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }, { v: 3 }]");
		age_past_ttl(&test);
		test.command("INSERT test::t [{ v: 4 }]");

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

		assert_eq!(row_count(&test, "from test::t"), 1, "only the row committed after the epoch survives");

		let eviction_version = test.current_version().unwrap();
		wait_cdc_watermark(&test, eviction_version);
		let cdc = test.ioc().try_resolve::<CdcStore>().unwrap();
		let record = cdc.read(eviction_version).unwrap();
		assert!(
			record.is_some_and(|r| !r.changes.is_empty()),
			"announced eviction must emit CDC changes for the removed rows"
		);
	}

	#[test]
	fn table_drop_mode_evicts_rows_silently_without_cdc() {
		// A silent TTL is the quiet variant: rows vanish but the eviction commit must not
		// produce any CDC record. This is the semantic difference the announce flag
		// exists for; if this fails, a silent eviction leaks deletes downstream.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", announce: false } } }",
		);
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);

		let before = test.current_version().unwrap();
		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtlSilent);
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
		// The whole point of transactional eviction: partition metadata is maintained in
		// the same commit as the row removals. A fully expired partition loses its
		// metadata key entirely (the Part 2 leak fix); a partially expired partition gets
		// count decremented and head advanced to the surviving row.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::rb { a: utf8, v: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", announce: true } }, partition: { by: { a } } }",
		);
		test.command("INSERT test::rb [{ a: \"us\", v: 1 }, { a: \"us\", v: 2 }, { a: \"us\", v: 3 }]");
		test.command("INSERT test::rb [{ a: \"eu\", v: 10 }, { a: \"eu\", v: 20 }]");
		age_past_ttl(&test);
		test.command("INSERT test::rb [{ a: \"eu\", v: 30 }]");

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

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
		// Non-partitioned ring buffers travel the empty-partition-values path: the same
		// metadata maintenance must apply to the whole-buffer metadata entry.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::rb { v: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.command("INSERT test::rb [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);
		test.command("INSERT test::rb [{ v: 3 }]");

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

		assert_eq!(row_count(&test, "from test::rb"), 1);
		let partitions = ringbuffer_partitions(&test, "rb");
		assert_eq!(partitions.len(), 1);
		assert_eq!(partitions[0].metadata.count, 1);

		age_past_ttl(&test);
		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

		assert_eq!(row_count(&test, "from test::rb"), 0);
		assert!(
			ringbuffer_partitions(&test, "rb").is_empty(),
			"a fully drained buffer must not leak a zero-count metadata entry"
		);
	}

	#[test]
	fn plain_ringbuffer_drop_mode_evicts_and_maintains_metadata() {
		// A silent TTL must still maintain ring buffer bookkeeping in the same commit even
		// though the row removal itself is silent; otherwise count/head desync and later
		// inserts/evictions misbehave (the original gc/row defect).
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::rb { v: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", announce: false } } }",
		);
		test.command("INSERT test::rb [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);
		test.command("INSERT test::rb [{ v: 3 }]");

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtlSilent);

		assert_eq!(row_count(&test, "from test::rb"), 1);
		let partitions = ringbuffer_partitions(&test, "rb");
		assert_eq!(partitions.len(), 1);
		assert_eq!(partitions[0].metadata.count, 1);
		assert_eq!(partitions[0].metadata.head, partitions[0].metadata.tail - 1);
	}

	#[test]
	fn budget_bounds_the_tick_and_cursor_resumes_on_the_next() {
		// One tick may evict at most batch_size x max_batches rows; the backlog must not
		// be lost but resume from the persisted cursor on the next tick. Without this the
		// evictor could stall a busy system in a single unbounded tick.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(2));
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }, { v: 5 }]");
		age_past_ttl(&test);

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);
		assert_eq!(
			row_count(&test, "from test::t"),
			1,
			"tick one is capped at 2 batches x 2 rows; one expired row must be left over"
		);

		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);
		assert_eq!(row_count(&test, "from test::t"), 0, "the cursor must resume and drain the leftover");
	}

	#[test]
	fn a_cutoff_the_clock_cannot_place_evicts_nothing() {
		// Expiry resolves to the instant `now - ttl`. When the clock sits closer to the start of
		// the representable range than the ttl is long, that subtraction underflows and there is
		// no cutoff to apply. The evictor must delete nothing rather than guess: a permissive
		// fallback (say, treating "unknown" as now) would delete rows whose age it cannot
		// establish, which is every row in the table.
		//
		// This replaces the old cold-epoch version of this test. A time cutoff needs no epoch
		// samples, so "the epoch holds nothing below the cutoff" is no longer reachable for row
		// ttl; underflow is the one remaining way the floor fails to resolve.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }]");

		let now = DateTime::from_nanos(HOUR.seconds() * 1_000_000_000 / 2);
		assert!(
			now.checked_sub(HOUR.to_duration()).is_none(),
			"precondition: the cutoff must be unresolvable, or this asserts nothing"
		);

		let mut state = EvictorState::default();
		Evictor::new((*test).clone()).run_tick(&mut state, RetentionClass::RowTtlAnnounced, now);

		assert_eq!(row_count(&test, "from test::t"), 2, "nothing may be evicted on a guess about age");
	}

	#[test]
	fn series_delete_mode_evicts_rows_and_decrements_row_count() {
		// Series parity: rows are evicted through the shared remove_series_row helper and
		// SeriesMetadata.row_count is decremented in the same commit, so the metadata can
		// never observe a state where rows are gone but the count still includes them.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create series test::s { ts: datetime, v: int4 } WITH { key: ts, row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.command(
			"INSERT test::s [{ ts: datetime::from_epoch_millis(1000), v: 1 }, { ts: datetime::from_epoch_millis(2000), v: 2 }]",
		);
		age_past_ttl(&test);
		test.command("INSERT test::s [{ ts: datetime::from_epoch_millis(3000), v: 3 }]");
		assert_eq!(series_metadata(&test, "s").row_count, 3);

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

		assert_eq!(row_count(&test, "from test::s"), 1);
		assert_eq!(
			series_metadata(&test, "s").row_count,
			1,
			"row_count must be decremented in the same commit as the row removals"
		);
	}

	#[test]
	fn dml_delete_and_evictor_produce_identical_ringbuffer_metadata() {
		// Pin for the Part 2 extraction: DML DELETE and the evictor share
		// apply_ringbuffer_partition_metadata_after_delete, so removing the same logical
		// rows from the same starting state must land on identical metadata. If this
		// diverges, the shared helper has forked.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::dml { a: utf8, v: int4 } WITH { capacity: 100, partition: { by: { a } } }",
		);
		test.admin(
			"CREATE RINGBUFFER test::evicted { a: utf8, v: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", announce: true } }, partition: { by: { a } } }",
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
		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

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
		// A ring buffer backing a deferred ringbuffer view (`underlying: true`) is written AND
		// evicted by its SinkRingBufferView operator, which owns both capacity and row-TTL eviction
		// on the flow tick. The retention evictor must NOT also reap it: doing so would strand the
		// operator's per-partition state (forward map, row entries, metadata) and bypass the
		// operator's downstream eviction propagation. So the evictor lists no partitions for an
		// underlying ring buffer even when the catalog holds partition metadata for it - while a
		// standalone `CREATE RINGBUFFER` (`underlying: false`, DML-written, no operator) stays
		// evictor-owned and is listed for eviction normally.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::src { base: utf8, n: int4 }");
		// The engine harness runs no flow subsystem, but the DDL still creates the view's backing
		// ring buffer with `underlying: true` and registers its row TTL settings.
		test.admin(
			"create deferred ringbuffer view test::rb { base: utf8, n: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", announce: false } }, partition: { by: { base } } } as { from test::src }",
		);
		test.admin(
			"CREATE RINGBUFFER test::standalone { base: utf8, n: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", announce: true } }, partition: { by: { base } } }",
		);
		test.command("INSERT test::standalone [{ base: \"us\", n: 1 }]");

		let underlying = underlying_ringbuffer(&test);
		let standalone = ringbuffer_by_name(&test, "standalone");

		// No flow populated the view's ring buffer, so seed one partition's metadata directly. This
		// makes the skip observable: without it, list_ringbuffer_partitions would be vacuously empty.
		let us = vec![Value::Utf8("us".to_string())];
		seed_partition(&test, underlying.id, us.clone());

		// The catalog itself holds the seeded partition for the underlying ring buffer ...
		assert_eq!(
			catalog_partition_values(&test, &underlying),
			vec![us.clone()],
			"the catalog must hold the seeded partition metadata (guards against a vacuous skip test)"
		);

		// ... yet the evictor lists nothing for it, because it is owned by the sink operator.
		assert!(
			evictor_partition_values(&test, underlying.id).is_empty(),
			"the retention evictor must skip underlying (view-backed) ring buffers"
		);

		// The standalone ring buffer is still evictor-owned, so its partition is listed.
		assert_eq!(
			evictor_partition_values(&test, standalone.id),
			vec![us],
			"a standalone ring buffer must remain owned by the retention evictor"
		);
	}

	#[test]
	fn budget_exhausted_with_a_live_cursor_yields_for_catchup() {
		// Pacing rule, Yielded direction. When a tick spends its whole batch budget and
		// expired rows remain behind a live cursor, the slice must report Yielded so the
		// lane's 5ms catch-up tick drains the backlog in milliseconds instead of waiting a
		// full eviction interval. Reporting Exhausted here is exactly the pacing defect the
		// task split exists to kill.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(2));
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }, { v: 5 }, { v: 6 }]");
		age_past_ttl(&test);

		let mut state = EvictorState::default();
		let progress = tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

		assert_eq!(
			progress,
			Progress::Yielded,
			"budget exhausted with a live cursor must yield so the catch-up tick drains the backlog"
		);
		assert_eq!(row_count(&test, "from test::t"), 2, "the tick is capped at 2 batches x 2 rows");

		let drained = tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);
		assert_eq!(
			drained,
			Progress::Exhausted,
			"once the backlog is gone the same slice must report Exhausted"
		);
		assert_eq!(row_count(&test, "from test::t"), 0);
	}

	#[test]
	fn budget_exhausted_at_an_object_boundary_yields_on_unvisited_work() {
		// The backlog hint must also count objects the budget never reached, not only objects
		// left mid-scan. Two announced tables with a budget of exactly one batch: the
		// first table drains cleanly (its cursor is removed), then the budget is spent and
		// the second table is never visited. If backlog counted only live cursors this tick
		// would wrongly report Exhausted and the untouched table would wait a full interval.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t1 { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.admin(
			"create table test::t2 { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(1));
		test.command("INSERT test::t1 [{ v: 1 }, { v: 2 }]");
		test.command("INSERT test::t2 [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);

		let mut state = EvictorState::default();
		let progress = tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

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
		// Idle cost rule. Re-confirming that an object has nothing expired must not consume
		// budget, or a tree with more objects than the budget can never finish a pass: the tick
		// reports Yielded, the lane respins at the 5ms catch-up cadence, and the maintenance
		// thread burns a core forever while reclaiming nothing. Four tables against a two-batch
		// budget is past that cliff - a table walks two keyspaces, so a charged pass needs eight.
		//
		// The epoch is recorded BEFORE the inserts so the cutoff resolves while every row sits
		// above it: the objects are eligible and are visited, they simply have nothing expired.
		// Recording after the inserts would expire them and this would assert nothing.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		for name in ["t1", "t2", "t3", "t4"] {
			test.admin(&format!(
				"create table test::{name} {{ v: int4 }} with {{ row: {{ ttl: {{ duration: \"1h\", announce: true }} }} }}"
			));
		}
		test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		test.set_config(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(2));
		age_past_ttl(&test);
		for name in ["t1", "t2", "t3", "t4"] {
			test.command(&format!("INSERT test::{name} [{{ v: 1 }}, {{ v: 2 }}]"));
		}

		let mut state = EvictorState::default();
		let progress = tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);

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
		// Fairness rule. A tick must resume where the previous one stopped rather than
		// restarting at the head of the object list. Without a resume cursor the budget is spent
		// on the same leading objects every tick and everything behind them starves
		// indefinitely - rows expire and are never reclaimed, which presents as an unbounded
		// memory leak rather than as a retention bug.
		//
		// Four tables each hold six expired rows against a budget of two batches of two, so no
		// single tick can reach past the first table. After four ticks a rotating evictor has
		// touched all four; a restarting one has touched only the first.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		for name in ["t1", "t2", "t3", "t4"] {
			test.admin(&format!(
				"create table test::{name} {{ v: int4 }} with {{ row: {{ ttl: {{ duration: \"1h\", announce: true }} }} }}"
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
			tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);
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
	fn unresolvable_floor_returns_exhausted_not_yielded() {
		// Pacing rule, Exhausted direction. When the cutoff cannot be resolved the class has no
		// eligible work and must report Exhausted. Yielding on a stuck floor would spin the lane
		// at the 5ms catch-up cadence and starve the other classes (landmine L7).
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }]");

		let now = DateTime::from_nanos(HOUR.seconds() * 1_000_000_000 / 2);
		assert!(
			now.checked_sub(HOUR.to_duration()).is_none(),
			"precondition: the cutoff must be unresolvable, or this asserts nothing"
		);

		let mut state = EvictorState::default();
		let progress = Evictor::new((*test).clone()).run_tick(&mut state, RetentionClass::RowTtlAnnounced, now);

		assert_eq!(progress, Progress::Exhausted, "an unresolvable floor must not spin the lane");
		assert_eq!(row_count(&test, "from test::t"), 2, "and nothing may be evicted on a guess about age");
	}

	#[test]
	fn a_slice_evicts_only_its_target_class() {
		// The task split gives each row-ttl class its own slice and budget. A silent slice
		// must leave announced tables untouched and vice versa, so a hot silent class can
		// never drag the announced class into re-ticking (and flooding the flow graph) with
		// it, which is the whole reason the classes are paced apart.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::silent { v: int4 } with { row: { ttl: { duration: \"1h\", announce: false } } }",
		);
		test.admin(
			"create table test::announced { v: int4 } with { row: { ttl: { duration: \"1h\", announce: true } } }",
		);
		test.command("INSERT test::silent [{ v: 1 }, { v: 2 }]");
		test.command("INSERT test::announced [{ v: 1 }, { v: 2 }]");
		age_past_ttl(&test);

		let mut state = EvictorState::default();
		tick_now(&test, &mut state, RetentionClass::RowTtlSilent);
		assert_eq!(row_count(&test, "from test::silent"), 0, "the silent slice must clear its own table");
		assert_eq!(
			row_count(&test, "from test::announced"),
			2,
			"the silent slice must not touch a announced table"
		);

		tick_now(&test, &mut state, RetentionClass::RowTtlAnnounced);
		assert_eq!(
			row_count(&test, "from test::announced"),
			0,
			"the announced slice clears the table its class owns"
		);
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
		// The evictor's entire work list is `list_row_settings`, which reads the catalog cache with
		// no storage fallback: an object missing from it is not evicted late, it is never considered
		// again, and unlike `find_row_settings` nothing warns. In a production run only the views
		// declaring `persistent: false` were ever scanned, so presence has to be proven independent
		// of that flag. A view declaring only `row: { ttl }` keeps its rows forever if it is absent
		// here, behind a TTL its own DDL advertises.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::src { base: utf8, n: int4 }");
		test.admin(
			"create deferred view test::implicit { base: utf8, n: int4 } with { row: { ttl: { duration: \"1h\" } } } as { from test::src }",
		);
		test.admin(
			"create deferred view test::explicit { base: utf8, n: int4 } with { row: { ttl: { duration: \"2h\" }, persistent: false } } as { from test::src }",
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
		// On restart the evictor's work list comes entirely from `load_all`. A row-settings entry
		// that reaches storage but not the rehydrated cache leaves that object silently perpetual for
		// the life of the process, with no storage fallback and no warning to reveal it. Loading a
		// fresh cache from the same store is that restart, minus the disk.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::src { base: utf8, n: int4 }");
		test.admin(
			"create deferred view test::implicit { base: utf8, n: int4 } with { row: { ttl: { duration: \"1h\" } } } as { from test::src }",
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
