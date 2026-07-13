// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	actors::retention::RetentionEvictMessage as Message,
	common::CommitVersion,
	event::row::RowsExpiredEvent,
	interface::{
		WithEventBus,
		catalog::{
			config::{ConfigKey, GetConfig},
			id::{RingBufferId, SeriesId, TableId},
			shape::ShapeId,
		},
	},
	key::{
		EncodableKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::RowKey,
		series_row::SeriesRowKeyRange,
	},
	row::{Ttl, TtlCleanupMode},
};
use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::{ActorConfig, ActorSpawner},
	timers::TimerHandle,
	traits::{Actor as ActorTrait, Directive},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{
	Value, datetime::DateTime, identity::IdentityId, partition::Partition, row_number::RowNumber,
};
use tracing::{debug, warn};

use crate::{
	Result,
	engine::StandardEngine,
	retention::scan,
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

type CursorKey = (ShapeId, EncodedKey);

pub struct EvictorState {
	_timer_handle: Option<TimerHandle>,
	running: bool,
	cursors: HashMap<CursorKey, EncodedKey>,
}

impl Default for EvictorState {
	fn default() -> Self {
		Self {
			_timer_handle: None,
			running: false,
			cursors: HashMap::new(),
		}
	}
}

#[derive(Default)]
struct TickStats {
	shapes_scanned: u64,
	shapes_skipped: u64,
	rows_expired: u64,
}

pub struct Evictor {
	engine: StandardEngine,
}

impl Evictor {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}

	#[tracing::instrument(name = "retention::evict::tick", level = "debug", skip_all)]
	fn run_tick(&self, state: &mut EvictorState, now: DateTime) {
		if state.running {
			debug!("retention eviction tick already in progress, skipping");
			return;
		}
		state.running = true;

		let catalog = self.engine.catalog();
		let batch_size = catalog.get_config_uint8(ConfigKey::RetentionEvictBatchSize) as usize;
		let mut budget = catalog.get_config_uint8(ConfigKey::RetentionEvictMaxBatchesPerTick);
		let mut stats = TickStats::default();

		for (shape, settings) in catalog.list_row_settings() {
			if budget == 0 {
				break;
			}
			let Some(ttl) = settings.ttl else {
				continue;
			};
			let Some(cutoff) = self.cutoff_version(now, &ttl) else {
				stats.shapes_skipped += 1;
				continue;
			};
			stats.shapes_scanned += 1;
			if let Err(e) = self.evict_shape(state, shape, &ttl, cutoff, batch_size, &mut budget, &mut stats)
			{
				warn!(?shape, error = %e, "retention eviction failed; resetting cursors, retrying next tick");
				state.cursors.retain(|key, _| key.0 != shape);
				budget = budget.saturating_sub(1);
			}
		}

		if stats.rows_expired > 0 {
			debug!(
				shapes_scanned = stats.shapes_scanned,
				shapes_skipped = stats.shapes_skipped,
				rows_expired = stats.rows_expired,
				"retention eviction tick completed"
			);
		}
		self.engine.event_bus().emit(RowsExpiredEvent::new(
			stats.shapes_scanned,
			stats.shapes_skipped,
			stats.rows_expired,
			0,
			HashMap::new(),
			HashMap::new(),
		));
		state.running = false;
	}

	fn cutoff_version(&self, now: DateTime, ttl: &Ttl) -> Option<CommitVersion> {
		let cutoff = now.checked_sub(ttl.duration)?;
		self.engine.version_epoch().floor_version_at(cutoff.to_nanos()).map(CommitVersion)
	}

	#[allow(clippy::too_many_arguments)]
	fn evict_shape(
		&self,
		state: &mut EvictorState,
		shape: ShapeId,
		ttl: &Ttl,
		cutoff: CommitVersion,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		match shape {
			ShapeId::Table(id) => {
				self.evict_table(state, id, &ttl.cleanup_mode, cutoff, batch_size, budget, stats)
			}
			ShapeId::RingBuffer(id) => {
				self.evict_ringbuffer(state, id, &ttl.cleanup_mode, cutoff, batch_size, budget, stats)
			}
			ShapeId::Series(id) => {
				self.evict_series(state, id, &ttl.cleanup_mode, cutoff, batch_size, budget, stats)
			}
			_ => Ok(()),
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn evict_table(
		&self,
		state: &mut EvictorState,
		id: TableId,
		mode: &TtlCleanupMode,
		cutoff: CommitVersion,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		let shape = ShapeId::Table(id);
		for keyspace in [RowKey::full_scan(shape), PartitionedRowKey::full_scan(shape)] {
			loop {
				if *budget == 0 {
					return Ok(());
				}
				*budget -= 1;
				let (rows, drained) =
					self.evict_table_batch(state, id, mode, cutoff, batch_size, &keyspace)?;
				stats.rows_expired += rows;
				if drained {
					break;
				}
			}
		}
		Ok(())
	}

	fn evict_table_batch(
		&self,
		state: &mut EvictorState,
		id: TableId,
		mode: &TtlCleanupMode,
		cutoff: CommitVersion,
		batch_size: usize,
		keyspace: &EncodedKeyRange,
	) -> Result<(u64, bool)> {
		let shape = ShapeId::Table(id);
		let cursor_key = (shape, scan::keyspace_start(keyspace));
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		let Some(table) = catalog.find_table(&mut Transaction::Command(&mut txn), id)? else {
			txn.rollback()?;
			state.cursors.retain(|key, _| key.0 != shape);
			return Ok((0, true));
		};

		let range = scan::resume_range(keyspace, state.cursors.get(&cursor_key));
		let result = scan::scan_expired(&mut txn, range, cutoff, batch_size, &|_| None)?;
		if result.expired.is_empty() {
			txn.rollback()?;
			return Ok((0, advance_cursor(state, cursor_key, result.next_cursor)));
		}

		let rows = result.expired.len() as u64;
		match mode {
			TtlCleanupMode::Delete => {
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
			TtlCleanupMode::Drop => {
				for row in &result.expired {
					txn.drop_key(&row.key)?;
				}
			}
		}
		txn.commit()?;
		Ok((rows, advance_cursor(state, cursor_key, result.next_cursor)))
	}

	#[allow(clippy::too_many_arguments)]
	fn evict_ringbuffer(
		&self,
		state: &mut EvictorState,
		id: RingBufferId,
		mode: &TtlCleanupMode,
		cutoff: CommitVersion,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		for partition_values in self.list_ringbuffer_partitions(id)? {
			loop {
				if *budget == 0 {
					return Ok(());
				}
				*budget -= 1;
				let (rows, drained) = self.evict_ringbuffer_partition_batch(
					state,
					id,
					mode,
					cutoff,
					batch_size,
					&partition_values,
				)?;
				stats.rows_expired += rows;
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
			let Some(ringbuffer) = catalog.find_ringbuffer(&mut Transaction::Command(&mut txn), id)?
			else {
				return Ok(Vec::new());
			};
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

	fn evict_ringbuffer_partition_batch(
		&self,
		state: &mut EvictorState,
		id: RingBufferId,
		mode: &TtlCleanupMode,
		cutoff: CommitVersion,
		batch_size: usize,
		partition_values: &[Value],
	) -> Result<(u64, bool)> {
		let shape = ShapeId::RingBuffer(id);
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		let Some(ringbuffer) = catalog.find_ringbuffer(&mut Transaction::Command(&mut txn), id)? else {
			txn.rollback()?;
			state.cursors.retain(|key, _| key.0 != shape);
			return Ok((0, true));
		};

		let partition = if ringbuffer.partition_by.is_empty() {
			None
		} else {
			Some(Partition::of(partition_values))
		};
		let keyspace = match partition {
			Some(partition) => PartitionedRowKey::partition_range(shape, partition),
			None => RowKey::full_scan(shape),
		};
		let cursor_key = (shape, scan::keyspace_start(&keyspace));

		let Some(metadata) =
			catalog.find_partition_metadata(&mut Transaction::Command(&mut txn), &ringbuffer, partition_values)?
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
		match mode {
			TtlCleanupMode::Delete => {
				for row in &result.expired {
					let Some(row_number) = decode_ringbuffer_row_number(&row.key, partitioned)
					else {
						continue;
					};
					txn.remove_from_ringbuffer(&ringbuffer, partition, RowNumber(row_number))?;
				}
			}
			TtlCleanupMode::Drop => {
				for row in &result.expired {
					txn.drop_key(&row.key)?;
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
	fn evict_series(
		&self,
		state: &mut EvictorState,
		id: SeriesId,
		mode: &TtlCleanupMode,
		cutoff: CommitVersion,
		batch_size: usize,
		budget: &mut u64,
		stats: &mut TickStats,
	) -> Result<()> {
		loop {
			if *budget == 0 {
				return Ok(());
			}
			*budget -= 1;
			let (rows, drained) = self.evict_series_batch(state, id, mode, cutoff, batch_size)?;
			stats.rows_expired += rows;
			if drained {
				return Ok(());
			}
		}
	}

	fn evict_series_batch(
		&self,
		state: &mut EvictorState,
		id: SeriesId,
		mode: &TtlCleanupMode,
		cutoff: CommitVersion,
		batch_size: usize,
	) -> Result<(u64, bool)> {
		let shape = ShapeId::Series(id);
		let catalog = self.engine.catalog();
		let mut txn = self.engine.begin_command(IdentityId::system())?;

		let Some(series) = catalog.find_series(&mut Transaction::Command(&mut txn), id)? else {
			txn.rollback()?;
			state.cursors.retain(|key, _| key.0 != shape);
			return Ok((0, true));
		};
		let Some(mut metadata) = catalog.find_series_metadata(&mut Transaction::Command(&mut txn), series.id)?
		else {
			txn.rollback()?;
			state.cursors.retain(|key, _| key.0 != shape);
			return Ok((0, true));
		};

		let partitioned = !series.partition_by.is_empty();
		let keyspace = if partitioned {
			PartitionedRowKey::full_scan(shape)
		} else {
			SeriesRowKeyRange::full_scan(series.id, None)
		};
		let cursor_key = (shape, scan::keyspace_start(&keyspace));

		let range = scan::resume_range(&keyspace, state.cursors.get(&cursor_key));
		let result = scan::scan_expired(&mut txn, range, cutoff, batch_size, &|_| None)?;
		if result.expired.is_empty() {
			txn.rollback()?;
			return Ok((0, advance_cursor(state, cursor_key, result.next_cursor)));
		}

		let deleted = result.expired.len() as u64;
		match mode {
			TtlCleanupMode::Delete => {
				let row_shape =
					get_or_create_series_shape(&catalog, &series, &mut Transaction::Command(&mut txn))?;
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
			TtlCleanupMode::Drop => {
				for row in &result.expired {
					txn.drop_key(&row.key)?;
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

impl ActorTrait for Evictor {
	type State = EvictorState;
	type Message = Message;

	fn init(&self, ctx: &Context<Message>) -> EvictorState {
		debug!("retention evictor started");
		let interval = self.engine.catalog().get_config_duration(ConfigKey::RetentionEvictInterval);
		let timer_handle = ctx.schedule_tick(interval, |nanos| Message::Tick(DateTime::from_nanos(nanos)));
		EvictorState {
			_timer_handle: Some(timer_handle),
			running: false,
			cursors: HashMap::new(),
		}
	}

	fn handle(&self, state: &mut EvictorState, msg: Message, ctx: &Context<Message>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}

		match msg {
			Message::Tick(now) => {
				self.run_tick(state, now);
			}
			Message::Shutdown => {
				debug!("retention evictor shutting down");
				return Directive::Stop;
			}
		}

		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("retention evictor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

pub fn spawn_retention_evictor(engine: StandardEngine, spawner: ActorSpawner) -> ActorRef<Message> {
	let actor = Evictor::new(engine);
	spawner.spawn_coordination("retention-evict", actor).actor_ref().clone()
}

#[cfg(test)]
mod tests {
	use std::time::Instant;

	use reifydb_cdc::{produce::watermark::CdcProducerWatermark, storage::CdcStore};
	use reifydb_core::interface::catalog::{ringbuffer::PartitionedMetadata, series::SeriesMetadata};

	use super::*;
	use crate::test_harness::TestEngine;

	const T0: u64 = 1_000_000_000_000;
	const HOUR: u64 = 3_600 * 1_000_000_000;
	const AFTER_TTL: u64 = T0 + HOUR + 1_000_000_000;

	// Expiry is version-anchored: a row is expired iff its commit version is at or below
	// the epoch sample recorded before `now - ttl`. Recording (T0, current_version) and
	// ticking at T0 + ttl + 1s expires exactly the rows committed up to the record call.
	fn record_epoch_now(engine: &StandardEngine) {
		let version = engine.current_version().unwrap();
		engine.version_epoch().record(T0, version.0);
	}

	fn tick(engine: &StandardEngine, state: &mut EvictorState, now_nanos: u64) {
		Evictor::new(engine.clone()).run_tick(state, DateTime::from_nanos(now_nanos));
	}

	fn row_count(test: &TestEngine, rql: &str) -> usize {
		TestEngine::row_count(&test.query(rql))
	}

	fn ringbuffer_partitions(engine: &StandardEngine, name: &str) -> Vec<PartitionedMetadata> {
		let catalog = engine.catalog();
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let namespace = catalog
			.find_namespace_by_name(&mut Transaction::Command(&mut txn), "test")
			.unwrap()
			.unwrap();
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
		let namespace = catalog
			.find_namespace_by_name(&mut Transaction::Command(&mut txn), "test")
			.unwrap()
			.unwrap();
		let series = catalog
			.find_series_by_name(&mut Transaction::Command(&mut txn), namespace.id(), name)
			.unwrap()
			.unwrap();
		let metadata =
			catalog.find_series_metadata(&mut Transaction::Command(&mut txn), series.id).unwrap().unwrap();
		txn.rollback().unwrap();
		metadata
	}

	fn set_config(engine: &StandardEngine, key: ConfigKey, value: Value) {
		let catalog = engine.catalog();
		let mut admin = engine.begin_admin(IdentityId::system()).unwrap();
		catalog.set_config(&mut admin, key, value).unwrap();
		admin.commit().unwrap();
	}

	fn wait_cdc_watermark(engine: &StandardEngine, version: CommitVersion) {
		let watermark = engine.ioc().try_resolve::<CdcProducerWatermark>().unwrap();
		let deadline = Instant::now() + std::time::Duration::from_secs(5);
		while watermark.get() < version {
			assert!(
				Instant::now() < deadline,
				"the cdc producer did not reach version {version:?} within the deadline"
			);
			std::thread::sleep(std::time::Duration::from_millis(10));
		}
	}

	#[test]
	fn table_delete_mode_evicts_expired_rows_transactionally() {
		// Delete mode must run through the engine operation helpers, so the eviction is a
		// real commit: rows disappear for readers and the commit produces a CDC record
		// (unlike the retired gc/row path, which bypassed the pipeline entirely).
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", mode: delete } } }",
		);
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }, { v: 3 }]");
		record_epoch_now(&test);
		test.command("INSERT test::t [{ v: 4 }]");

		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);

		assert_eq!(row_count(&test, "from test::t"), 1, "only the row committed after the epoch survives");

		let eviction_version = test.current_version().unwrap();
		wait_cdc_watermark(&test, eviction_version);
		let cdc = test.ioc().try_resolve::<CdcStore>().unwrap();
		let record = cdc.read(eviction_version).unwrap();
		assert!(
			record.is_some_and(|r| !r.changes.is_empty()),
			"delete-mode eviction must emit CDC changes for the removed rows"
		);
	}

	#[test]
	fn table_drop_mode_evicts_rows_silently_without_cdc() {
		// Drop mode is the silent variant: rows vanish but the eviction commit must not
		// produce any CDC record. This is the semantic difference the TtlCleanupMode
		// distinction exists for; if this fails, drop mode leaks deletes downstream.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin("create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", mode: drop } } }");
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }]");
		record_epoch_now(&test);

		let before = test.current_version().unwrap();
		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);
		let after = test.current_version().unwrap();

		assert_eq!(row_count(&test, "from test::t"), 0);
		assert!(after > before, "drop-mode eviction must still be a real commit");

		wait_cdc_watermark(&test, after);
		let cdc = test.ioc().try_resolve::<CdcStore>().unwrap();
		for version in (before.0 + 1)..=after.0 {
			assert!(
				cdc.read(CommitVersion(version)).unwrap().is_none(),
				"drop-mode eviction of a plain table must not write any CDC record"
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
			"CREATE RINGBUFFER test::rb { a: utf8, v: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", mode: delete } }, partition: { by: { a } } }",
		);
		test.command("INSERT test::rb [{ a: \"us\", v: 1 }, { a: \"us\", v: 2 }, { a: \"us\", v: 3 }]");
		test.command("INSERT test::rb [{ a: \"eu\", v: 10 }, { a: \"eu\", v: 20 }]");
		record_epoch_now(&test);
		test.command("INSERT test::rb [{ a: \"eu\", v: 30 }]");

		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);

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
			"CREATE RINGBUFFER test::rb { v: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", mode: delete } } }",
		);
		test.command("INSERT test::rb [{ v: 1 }, { v: 2 }]");
		record_epoch_now(&test);
		test.command("INSERT test::rb [{ v: 3 }]");

		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);

		assert_eq!(row_count(&test, "from test::rb"), 1);
		let partitions = ringbuffer_partitions(&test, "rb");
		assert_eq!(partitions.len(), 1);
		assert_eq!(partitions[0].metadata.count, 1);

		record_epoch_now(&test);
		tick(&test, &mut state, AFTER_TTL);

		assert_eq!(row_count(&test, "from test::rb"), 0);
		assert!(
			ringbuffer_partitions(&test, "rb").is_empty(),
			"a fully drained buffer must not leak a zero-count metadata entry"
		);
	}

	#[test]
	fn plain_ringbuffer_drop_mode_evicts_and_maintains_metadata() {
		// Drop mode must still maintain ring buffer bookkeeping in the same commit even
		// though the row removal itself is silent; otherwise count/head desync and later
		// inserts/evictions misbehave (the original gc/row defect).
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"CREATE RINGBUFFER test::rb { v: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", mode: drop } } }",
		);
		test.command("INSERT test::rb [{ v: 1 }, { v: 2 }]");
		record_epoch_now(&test);
		test.command("INSERT test::rb [{ v: 3 }]");

		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);

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
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", mode: delete } } }",
		);
		set_config(&test, ConfigKey::RetentionEvictBatchSize, Value::Uint8(2));
		set_config(&test, ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(2));
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }, { v: 5 }]");
		record_epoch_now(&test);

		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);
		assert_eq!(
			row_count(&test, "from test::t"),
			1,
			"tick one is capped at 2 batches x 2 rows; one expired row must be left over"
		);

		tick(&test, &mut state, AFTER_TTL);
		assert_eq!(row_count(&test, "from test::t"), 0, "the cursor must resume and drain the leftover");
	}

	#[test]
	fn cold_epoch_evicts_nothing() {
		// With no epoch sample at or below now - ttl there is no safe cutoff version, so
		// the evictor must not guess: deleting against a cold epoch could evict rows that
		// are younger than the TTL. Built without CDC because the CDC test harness also
		// registers the VersionEpochListener, which would warm the epoch on every commit.
		let test = TestEngine::builder().build();
		test.admin("create namespace test;");
		test.admin(
			"create table test::t { v: int4 } with { row: { ttl: { duration: \"1h\", mode: delete } } }",
		);
		test.command("INSERT test::t [{ v: 1 }, { v: 2 }]");

		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);

		assert_eq!(row_count(&test, "from test::t"), 2, "a cold epoch must leave every row in place");
	}

	#[test]
	fn series_delete_mode_evicts_rows_and_decrements_row_count() {
		// Series parity: rows are evicted through the shared remove_series_row helper and
		// SeriesMetadata.row_count is decremented in the same commit, so the metadata can
		// never observe a state where rows are gone but the count still includes them.
		let test = TestEngine::new();
		test.admin("create namespace test;");
		test.admin(
			"create series test::s { ts: datetime, v: int4 } WITH { key: ts, row: { ttl: { duration: \"1h\", mode: delete } } }",
		);
		test.command(
			"INSERT test::s [{ ts: datetime::from_epoch_millis(1000), v: 1 }, { ts: datetime::from_epoch_millis(2000), v: 2 }]",
		);
		record_epoch_now(&test);
		test.command("INSERT test::s [{ ts: datetime::from_epoch_millis(3000), v: 3 }]");
		assert_eq!(series_metadata(&test, "s").row_count, 3);

		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);

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
			"CREATE RINGBUFFER test::evicted { a: utf8, v: int4 } WITH { capacity: 100, row: { ttl: { duration: \"1h\", mode: delete } }, partition: { by: { a } } }",
		);
		for rql in [
			"INSERT test::dml [{ a: \"us\", v: 0 }, { a: \"us\", v: 1 }, { a: \"us\", v: 2 }]",
			"INSERT test::evicted [{ a: \"us\", v: 0 }, { a: \"us\", v: 1 }, { a: \"us\", v: 2 }]",
		] {
			test.command(rql);
		}
		record_epoch_now(&test);
		for rql in [
			"INSERT test::dml [{ a: \"us\", v: 3 }, { a: \"us\", v: 4 }]",
			"INSERT test::evicted [{ a: \"us\", v: 3 }, { a: \"us\", v: 4 }]",
		] {
			test.command(rql);
		}

		test.command("DELETE test::dml FILTER v < 3");
		let mut state = EvictorState::default();
		tick(&test, &mut state, AFTER_TTL);

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
}
