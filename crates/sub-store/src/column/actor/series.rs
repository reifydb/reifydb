// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	ops::Range,
	sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_schema::{ArrowError, FieldRef};
use arrow_select::take::take;
use reifydb_catalog::{catalog::Catalog, store::column_block::create::ColumnBlockToCreate};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			column_block::{ColumnBlock, ColumnBlockSource, ColumnStats},
			id::SeriesId,
			object::ObjectId,
			series::{Series, SeriesPartitionMetadata},
			storage::StorageId,
		},
		resolved::{ResolvedNamespace, ResolvedSeries},
	},
	key::{
		any::TaggedKey,
		partition::PartitionKey,
		series::{PartitionedSeriesRowKeyRange, SeriesRowKeyRange},
	},
	partition::registry_values,
	retry::is_conflict,
	value::batch::concat_columns,
};
use reifydb_engine::{
	engine::StandardEngine,
	vm::volcano::{
		query::{QueryContext, QueryNode, query_budget},
		scan::{
			column_decode::{BlockDecoder, Decoded},
			column_encode::{compress_columns, encode_series_block},
			series::SeriesScanNode,
		},
	},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_extension::block_encoding::registry::BlockEncodings;
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	timers::TimerHandle,
	traits::{Actor, Directive},
};
use reifydb_store_column::{
	block::BlockData,
	bucket::{Bucket, bucket_for, is_closed},
	compress::Compressor,
	convert::to_arrow,
	device::BlockFileKey,
	store::ColumnStore,
};
use reifydb_transaction::{
	multi::RangeScope,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	reifydb_assertions,
	value::{
		Value, datetime::DateTime, duration::Duration, identity::IdentityId, partition::Partition,
		system_columns::SystemColumn, value_type::ValueType,
	},
};
use tracing::debug;

use crate::column::{
	actor::{
		SeriesMessage,
		batches::{arrow_columns_from_batches, stats_from_arrow, system_column_schema},
	},
	error::SubStoreError,
};

pub struct SeriesMaterializationState {
	pub partitions: HashMap<SeriesId, Vec<(Partition, Vec<Value>)>>,
	pub unnamed_files: HashMap<(u64, u64), DateTime>,
	_timer_handle: Option<TimerHandle>,
}

pub struct SeriesMaterializationActor {
	engine: StandardEngine,
	block_store: ColumnStore,
	compressor: Compressor,
	block_encodings: BlockEncodings,
	tick_interval: Duration,
	bucket_width: u64,
	grace: Duration,
	merge_buckets: u64,
	merge_file_delay: Duration,
}

#[derive(Clone, Copy)]
struct PartitionScope<'a> {
	partition: Partition,
	values: &'a [Value],
	metadata: &'a SeriesPartitionMetadata,
}

impl SeriesMaterializationActor {
	pub fn new(
		engine: StandardEngine,
		block_store: ColumnStore,
		compressor: Compressor,
		block_encodings: BlockEncodings,
		tick_interval: Duration,
		bucket_width: u64,
		grace: Duration,
		merge_buckets: u64,
		merge_file_delay: Duration,
	) -> Self {
		Self {
			engine,
			block_store,
			compressor,
			block_encodings,
			tick_interval,
			bucket_width,
			grace,
			merge_buckets,
			merge_file_delay,
		}
	}

	pub fn block_store(&self) -> &ColumnStore {
		&self.block_store
	}

	fn run_tick(&self, state: &mut SeriesMaterializationState, _now: DateTime) {
		let mut query_txn = match self.engine.begin_query(IdentityId::system()) {
			Ok(txn) => txn,
			Err(e) => panic!("series materialization: begin_query failed: {e}"),
		};
		let catalog = self.engine.catalog();
		let now_wall = self.wall_clock_now();
		let series_list = match catalog.list_series(&mut Transaction::Query(&mut query_txn)) {
			Ok(series_list) => series_list,
			Err(e) => panic!("series materialization: list_series failed: {e}"),
		};
		if let Err(e) = self.sweep_unnamed_files(state, &mut query_txn, &catalog, now_wall) {
			panic!("series materialization: sweeping unnamed block files failed: {e}");
		}
		state.partitions.clear();
		for series in series_list {
			if let Err(e) = skip_on_conflict(self.materialize_series_buckets(
				state,
				&mut query_txn,
				&catalog,
				&series,
				now_wall,
			)) {
				panic!(
					"series materialization failed for series {:?} ({}): {e}",
					series.id, series.name
				);
			}
		}
	}

	#[inline]
	fn wall_clock_now(&self) -> DateTime {
		self.engine.clock().now()
	}

	fn materialize_series_buckets(
		&self,
		state: &mut SeriesMaterializationState,
		query_txn: &mut QueryTransaction,
		catalog: &Catalog,
		series: &Series,
		now_wall: DateTime,
	) -> Result<()> {
		let partitions = self.partitions_of(state, query_txn, series)?;
		for (partition, partition_values) in partitions {
			let Some(metadata) = catalog.find_series_metadata(
				&mut Transaction::Query(&mut *query_txn),
				series.id,
				partition,
			)?
			else {
				continue;
			};
			if metadata.row_count == 0 {
				self.drop_partition_blocks(series, partition)?;
				continue;
			}
			let first = bucket_for(metadata.oldest_key, self.bucket_width);
			let last = bucket_for(metadata.newest_key, self.bucket_width);
			let live = first.start..last.end;
			if metadata.dirty_from_key < live.start || metadata.dirty_to_key > live.end {
				self.drop_blocks_outside(series, partition, live.clone())?;
			}
			let dirty = metadata.dirty_from_key.max(live.start)..metadata.dirty_to_key.min(live.end);
			let mut deferred = false;
			let scope = PartitionScope {
				partition,
				values: &partition_values,
				metadata: &metadata,
			};
			let mut start = bucket_for(dirty.start, self.bucket_width).start;
			while start < dirty.end {
				let bucket = Bucket {
					start,
					end: start + self.bucket_width,
					width: self.bucket_width,
				};
				start = start.saturating_add(self.bucket_width);
				if !is_closed(&bucket, series, scope.metadata, now_wall, self.grace) {
					deferred = true;
					continue;
				}
				self.materialize_bucket(series, scope, &bucket)?;
			}
			if !deferred {
				self.clear_dirty_mark(series, partition, &metadata)?;
			}
			self.merge_windows(series, scope, now_wall)?;
		}
		Ok(())
	}

	fn drop_partition_blocks(&self, series: &Series, partition: Partition) -> Result<()> {
		if self.drop_blocks_outside(series, partition, 0..0)? {
			self.reset_dirty_mark(series, partition)?;
		}
		Ok(())
	}

	fn drop_blocks_outside(&self, series: &Series, partition: Partition, live: Range<u64>) -> Result<bool> {
		let stored_partition = if series.partition_by.is_empty() {
			None
		} else {
			Some(partition)
		};
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let catalog = self.engine.catalog();
		let stale: Vec<ColumnBlock> = catalog
			.list_column_blocks_for_series(&mut Transaction::Admin(&mut admin), series.id)?
			.into_iter()
			.filter(|block| match block.source {
				ColumnBlockSource::Series {
					partition,
					start,
					end,
					..
				} => partition == stored_partition && (end <= live.start || start >= live.end),
				_ => false,
			})
			.collect();
		if stale.is_empty() {
			return Ok(false);
		}
		for block in &stale {
			catalog.drop_column_block(&mut admin, block.id)?;
		}
		commit_admin(admin)?;
		for block in &stale {
			self.block_store.remove(&BlockFileKey::of(block))?;
		}
		Ok(true)
	}

	fn reset_dirty_mark(&self, series: &Series, partition: Partition) -> Result<()> {
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let catalog = self.engine.catalog();
		let mut tx = Transaction::Admin(&mut admin);
		let Some(mut metadata) = catalog.find_series_metadata(&mut tx, series.id, partition)? else {
			return Ok(());
		};
		if metadata.dirty_from_key == u64::MAX && metadata.dirty_to_key == 0 {
			return Ok(());
		}
		metadata.dirty_from_key = u64::MAX;
		metadata.dirty_to_key = 0;
		catalog.update_series_metadata_rebased_txn(&mut tx, series.id, partition, metadata, |latest| {
			latest.dirty_from_key = u64::MAX;
			latest.dirty_to_key = 0;
			Ok(())
		})?;
		commit_admin(admin)
	}

	fn clear_dirty_mark(
		&self,
		series: &Series,
		partition: Partition,
		observed: &SeriesPartitionMetadata,
	) -> Result<()> {
		if observed.dirty_from_key >= observed.dirty_to_key {
			return Ok(());
		}
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let catalog = self.engine.catalog();
		let mut tx = Transaction::Admin(&mut admin);
		let Some(mut metadata) = catalog.find_series_metadata(&mut tx, series.id, partition)? else {
			return Ok(());
		};
		if !unchanged_since_scan(&metadata, observed) {
			return Ok(());
		}
		metadata.dirty_from_key = u64::MAX;
		metadata.dirty_to_key = 0;
		let observed = observed.clone();
		catalog.update_series_metadata_rebased_txn(&mut tx, series.id, partition, metadata, move |latest| {
			if unchanged_since_scan(latest, &observed) {
				latest.dirty_from_key = u64::MAX;
				latest.dirty_to_key = 0;
			}
			Ok(())
		})?;
		commit_admin(admin)
	}

	fn partitions_of(
		&self,
		state: &mut SeriesMaterializationState,
		query_txn: &mut QueryTransaction,
		series: &Series,
	) -> Result<Vec<(Partition, Vec<Value>)>> {
		if let Some(cached) = state.partitions.get(&series.id) {
			return Ok(cached.clone());
		}
		let partitions = if series.partition_by.is_empty() {
			vec![(Partition::default(), Vec::new())]
		} else {
			self.read_partition_registry(query_txn, series)?
		};
		state.partitions.insert(series.id, partitions.clone());
		Ok(partitions)
	}

	fn read_partition_registry(
		&self,
		query_txn: &mut QueryTransaction,
		series: &Series,
	) -> Result<Vec<(Partition, Vec<Value>)>> {
		let mut partitions = Vec::new();
		let mut tx = Transaction::Query(query_txn);
		let stream = tx.range(PartitionKey::full_scan(ObjectId::Series(series.id)), RangeScope::All, 1024)?;
		for entry in stream {
			let entry = entry?;
			if let TaggedKey::Partition(key) = entry.key {
				partitions.push((key.partition, registry_values(&entry.bytes)));
			}
		}
		Ok(partitions)
	}

	fn materialize_bucket(&self, series: &Series, scope: PartitionScope<'_>, bucket: &Bucket) -> Result<()> {
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let sealed_at_commit_version = admin.version();
		let resolved_series = self.resolve_series_target(&mut Transaction::Admin(&mut admin), series)?;
		let tags = self.bucket_scan_tags(&mut Transaction::Admin(&mut admin), series)?;
		let batches =
			self.scan_bucket_batches(&mut admin, resolved_series, scope.partition, series, bucket, &tags)?;
		if batches.iter().all(|batch| batch.num_rows() == 0) {
			return Ok(());
		}

		let storage = StorageId::series(series.id);
		let tagged = series.tag.is_some();
		let mut keys = Vec::new();
		let mut tx = Transaction::Admin(&mut admin);
		for tag in &tags {
			let range = if series.partition_by.is_empty() {
				SeriesRowKeyRange::scan_range(
					storage,
					tagged,
					*tag,
					Some(bucket.start),
					Some(bucket.end),
					None,
				)
			} else {
				PartitionedSeriesRowKeyRange::scan_range(
					storage,
					scope.partition,
					tagged,
					*tag,
					Some(bucket.start),
					Some(bucket.end),
					None,
				)
			};
			for row in tx.range(range, RangeScope::All, 1024)? {
				keys.push(row?.key);
			}
		}

		let (data, stats) = self.build_column_block(series, batches, sealed_at_commit_version)?;

		reifydb_assertions! {
			assert_eq!(
				keys.len(),
				data.len(),
				"the seal must drain exactly the rows its block holds, otherwise rows are lost or stored twice"
			);
		}

		self.create_block_and_drain_rows(&mut admin, series, scope, &stats, bucket, &keys, Arc::new(data))?;
		commit_admin(admin)
	}

	#[inline]
	fn resolve_series_target(&self, tx: &mut Transaction<'_>, series: &Series) -> Result<ResolvedSeries> {
		let catalog = self.engine.catalog();
		let namespace_def =
			catalog.find_namespace(tx, series.namespace)?.ok_or_else(|| missing_namespace(series))?;
		let resolved_namespace =
			ResolvedNamespace::new(Fragment::internal(namespace_def.name()), namespace_def.clone());
		Ok(ResolvedSeries::new(Fragment::internal(series.name.clone()), resolved_namespace, series.clone()))
	}

	#[inline]
	fn scan_bucket_batches(
		&self,
		admin: &mut AdminTransaction,
		resolved_series: ResolvedSeries,
		partition: Partition,
		series: &Series,
		bucket: &Bucket,
		tags: &[Option<u8>],
	) -> Result<Vec<RecordBatch>> {
		let services = self.engine.services();
		let memory = query_budget(&services);
		let context = Arc::new(QueryContext {
			services,
			source: None,
			batch_size: 1024,
			params: Params::None,
			symbols: SymbolTable::new(),
			identity: IdentityId::system(),
			memory,
		});

		let scan_partition = if series.partition_by.is_empty() {
			None
		} else {
			Some(partition)
		};
		let mut tx = Transaction::Admin(admin);
		let mut ctx = (*context).clone();
		let mut batches = Vec::new();
		for tag in tags {
			let mut scan = SeriesScanNode::new(
				resolved_series.clone(),
				Some(bucket.start),
				Some(bucket.end),
				*tag,
				scan_partition,
				Arc::clone(&context),
			)?;
			scan.initialize(&mut tx, &context)?;
			while let Some(batch) = scan.next(&mut tx, &mut ctx)? {
				batches.push(batch);
			}
		}
		Ok(batches)
	}

	#[inline]
	fn bucket_scan_tags(&self, tx: &mut Transaction<'_>, series: &Series) -> Result<Vec<Option<u8>>> {
		let Some(sumtype) = series.tag else {
			return Ok(vec![None]);
		};
		let definition =
			self.engine.catalog().find_sumtype(tx, sumtype)?.ok_or(SubStoreError::SumTypeMissing {
				sumtype,
				series: series.id,
			})?;
		let mut tags: Vec<u8> = definition.variants.iter().map(|variant| variant.tag).collect();
		tags.sort_unstable_by(|left, right| right.cmp(left));
		Ok(tags.into_iter().map(Some).collect())
	}

	#[inline]
	fn build_column_block(
		&self,
		series: &Series,
		batches: Vec<RecordBatch>,
		version: CommitVersion,
	) -> Result<(BlockData, Vec<ColumnStats>)> {
		let schema = scan_output_schema(series);
		self.encode_columns(series, arrow_columns_from_batches(&schema, &batches, version)?)
	}

	fn encode_columns(
		&self,
		series: &Series,
		columns: Vec<(String, ValueType, (FieldRef, ArrayRef))>,
	) -> Result<(BlockData, Vec<ColumnStats>)> {
		let stats = stats_from_arrow(&columns, self.block_store.session())?;
		let data = match &series.encoding {
			None => compress_columns(columns, &self.compressor)?,
			Some(encoding) => {
				encode_series_block(columns, series, encoding, &self.block_encodings, &self.compressor)?
			}
		};
		Ok((data, stats))
	}

	fn sweep_unnamed_files(
		&self,
		state: &mut SeriesMaterializationState,
		query_txn: &mut QueryTransaction,
		catalog: &Catalog,
		now: DateTime,
	) -> Result<()> {
		let named: HashSet<(u64, u64)> = catalog
			.list_column_blocks(&mut Transaction::Query(query_txn))?
			.iter()
			.map(|block| {
				let key = BlockFileKey::of(block);
				(key.dir, key.name)
			})
			.collect();
		let delay = self.merge_file_delay.to_std();
		let mut unnamed = HashMap::new();
		for key in self.block_store.list()? {
			let id = (key.dir, key.name);
			if named.contains(&id) {
				continue;
			}
			let first_seen = state.unnamed_files.get(&id).copied().unwrap_or(now);
			if now.saturating_duration_since(first_seen).to_std() >= delay {
				self.block_store.remove(&key)?;
			} else {
				unnamed.insert(id, first_seen);
			}
		}
		state.unnamed_files = unnamed;
		Ok(())
	}

	fn merge_windows(&self, series: &Series, scope: PartitionScope<'_>, now_wall: DateTime) -> Result<()> {
		if self.merge_buckets == 0 {
			return Ok(());
		}
		let window_width = self.bucket_width.saturating_mul(self.merge_buckets);
		let mut query_txn = self.engine.begin_query(IdentityId::system())?;
		let blocks = self.engine.catalog().list_column_blocks_for_series_partition(
			&mut Transaction::Query(&mut query_txn),
			series.id,
			scope.partition,
		)?;
		let mut windows: BTreeMap<u64, Vec<ColumnBlock>> = BTreeMap::new();
		for block in blocks {
			if let ColumnBlockSource::Series {
				start,
				..
			} = block.source
			{
				windows.entry(start - start % window_width).or_default().push(block);
			}
		}
		for (window_start, inputs) in windows {
			if inputs.len() < 2 {
				continue;
			}
			let window = window_start..window_start.saturating_add(window_width);
			let last = Bucket {
				start: window.end - self.bucket_width,
				end: window.end,
				width: self.bucket_width,
			};
			if !is_closed(&last, series, scope.metadata, now_wall, self.grace) {
				continue;
			}
			self.merge_window(series, scope, window, &inputs)?;
		}
		Ok(())
	}

	fn merge_window(
		&self,
		series: &Series,
		scope: PartitionScope<'_>,
		window: Range<u64>,
		inputs: &[ColumnBlock],
	) -> Result<()> {
		let decoder = BlockDecoder::new(self.block_encodings.clone(), self.block_store.session().clone());
		let mut per_block = Vec::with_capacity(inputs.len());
		for block in inputs {
			let stored = self
				.block_store
				.open(&BlockFileKey::of(block))?
				.ok_or(SubStoreError::BlockFileMissing {
					block: block.id,
				})?
				.read(None)?;
			let Decoded::Block(data) = decoder.decode(stored, None)? else {
				return Err(SubStoreError::BlockFileMissing {
					block: block.id,
				}
				.into());
			};
			per_block.push(self.block_columns(&data)?);
		}
		let schema: Vec<(String, ValueType)> =
			per_block[0].iter().map(|(name, ty, _)| (name.clone(), ty.clone())).collect();
		if per_block.iter().any(|columns| {
			columns.iter().map(|(name, ty, _)| (name, ty)).ne(schema.iter().map(|(name, ty)| (name, ty)))
		}) {
			return Ok(());
		}
		let mut merged = Vec::with_capacity(schema.len());
		for (index, (name, ty)) in schema.iter().enumerate() {
			let parts: Vec<(FieldRef, ArrayRef)> =
				per_block.iter().map(|columns| columns[index].2.clone()).collect();
			merged.push((name.clone(), ty.clone(), concat_columns(&parts)?));
		}
		let order = merge_order(series, &merged)?;
		let mut sorted = Vec::with_capacity(merged.len());
		for (name, ty, (field, array)) in merged {
			sorted.push((name, ty, (field, take(array.as_ref(), &order, None).map_err(arrow_error)?)));
		}
		let row_count: u64 = inputs.iter().map(|block| block.row_count).sum();
		let (data, stats) = self.encode_columns(series, sorted)?;

		reifydb_assertions! {
			assert_eq!(
				data.len() as u64,
				row_count,
				"a merge must keep every input row exactly once, otherwise rows are lost or duplicated"
			);
		}

		let stored_partition = if series.partition_by.is_empty() {
			None
		} else {
			Some(scope.partition)
		};
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let catalog = self.engine.catalog();
		let block = catalog.create_column_block(
			&mut admin,
			ColumnBlockToCreate {
				namespace: series.namespace,
				source: ColumnBlockSource::Series {
					series_id: series.id,
					partition: stored_partition,
					start: window.start,
					end: window.end,
				},
				row_count,
				partition_values: scope.values.to_vec(),
				stats,
			},
		)?;
		for input in inputs {
			catalog.drop_column_block(&mut admin, input.id)?;
		}
		self.block_store.write(&BlockFileKey::of(&block), &data)?;
		commit_admin(admin)
	}

	fn block_columns(&self, data: &BlockData) -> Result<Vec<(String, ValueType, (FieldRef, ArrayRef))>> {
		let session = self.block_store.session();
		let mut columns = Vec::with_capacity(data.columns.len());
		for ((name, ty, _), chunks) in data.schema.iter().zip(&data.columns) {
			let mut parts = Vec::with_capacity(chunks.chunks.len());
			for chunk in &chunks.chunks {
				parts.push(to_arrow(session, name, &chunks.field_type, chunk.clone())?);
			}
			columns.push((name.clone(), ty.clone(), concat_columns(&parts)?));
		}
		Ok(columns)
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn create_block_and_drain_rows(
		&self,
		admin: &mut AdminTransaction,
		series: &Series,
		scope: PartitionScope<'_>,
		stats: &[ColumnStats],
		bucket: &Bucket,
		keys: &[TaggedKey],
		data: Arc<BlockData>,
	) -> Result<()> {
		let stored_partition = if series.partition_by.is_empty() {
			None
		} else {
			Some(scope.partition)
		};
		let block = self.engine.catalog().create_column_block(
			admin,
			ColumnBlockToCreate {
				namespace: series.namespace,
				source: ColumnBlockSource::Series {
					series_id: series.id,
					partition: stored_partition,
					start: bucket.start,
					end: bucket.end,
				},
				row_count: data.len() as u64,
				partition_values: scope.values.to_vec(),
				stats: stats.to_vec(),
			},
		)?;
		for key in keys {
			admin.remove_silent(key)?;
		}
		self.block_store.write(&BlockFileKey::of(&block), data.as_ref())
	}
}

fn skip_on_conflict(result: Result<()>) -> Result<()> {
	match result {
		Err(e) if is_conflict(&e) => Ok(()),
		other => other,
	}
}

fn unchanged_since_scan(current: &SeriesPartitionMetadata, observed: &SeriesPartitionMetadata) -> bool {
	current == observed
}

fn commit_admin(mut admin: AdminTransaction) -> Result<()> {
	admin.commit()?;
	Ok(())
}

fn scan_output_schema(series: &Series) -> Vec<(String, ValueType)> {
	let key_name = series.key.column().to_string();
	let key_ty = series
		.columns
		.iter()
		.find(|c| c.name == key_name)
		.map(|c| c.constraint.get_type())
		.unwrap_or(ValueType::Uint8);

	let system = system_column_schema(&series.time, !series.partition_by.is_empty());
	let mut schema = Vec::with_capacity(series.columns.len() + 1 + system.len());
	schema.push((key_name.clone(), key_ty));
	if series.tag.is_some() {
		schema.push(("tag".to_string(), ValueType::Uint1));
	}
	for col in series.data_columns() {
		schema.push((col.name.clone(), col.constraint.get_type()));
	}
	schema.extend(system);
	schema
}

fn missing_namespace(series: &Series) -> SubStoreError {
	SubStoreError::NamespaceMissing {
		namespace: series.namespace,
		series: series.id,
	}
}

impl Actor for SeriesMaterializationActor {
	type State = SeriesMaterializationState;
	type Message = SeriesMessage;

	fn init(&self, ctx: &Context<SeriesMessage>) -> SeriesMaterializationState {
		debug!(
			"SeriesMaterializationActor started (tick={:?}, width={}, grace={:?})",
			self.tick_interval, self.bucket_width, self.grace
		);
		let handle =
			ctx.schedule_tick(self.tick_interval, |nanos| SeriesMessage::Tick(DateTime::from_nanos(nanos)));
		SeriesMaterializationState {
			partitions: HashMap::new(),
			unnamed_files: HashMap::new(),
			_timer_handle: Some(handle),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}
		match msg {
			SeriesMessage::Tick(now) => self.run_tick(state, now),
			SeriesMessage::Shutdown => {
				debug!("SeriesMaterializationActor shutting down");
				return Directive::Stop;
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("SeriesMaterializationActor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}

fn merge_order(series: &Series, columns: &[(String, ValueType, (FieldRef, ArrayRef))]) -> Result<UInt32Array> {
	let descending = Some(SortOptions {
		descending: true,
		nulls_first: false,
	});
	let mut keys = Vec::with_capacity(3);
	let names = series.tag.map(|_| "tag").into_iter().chain([series.key.column(), SystemColumn::RowNumbers.name()]);
	for name in names {
		let (_, _, (_, array)) = columns.iter().find(|(column, _, _)| column == name).ok_or_else(|| {
			SubStoreError::MissingColumnInBatch {
				column: name.to_string(),
			}
		})?;
		keys.push(SortColumn {
			values: array.clone(),
			options: descending,
		});
	}
	lexsort_to_indices(&keys, None).map_err(arrow_error)
}

fn arrow_error(e: ArrowError) -> Error {
	SubStoreError::Arrow {
		reason: e.to_string(),
	}
	.into()
}

#[cfg(test)]
mod tests {
	use std::{cell::Cell, thread::scope};

	use reifydb_core::interface::catalog::series::SeriesPartitionMetadata;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::error::TransactionError;
	use reifydb_value::params;

	use super::*;

	fn stored_metadata(engine: &StandardEngine) -> SeriesPartitionMetadata {
		let catalog = engine.catalog();
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let mut tx = Transaction::Command(&mut txn);
		let namespace = catalog.find_namespace_by_name(&mut tx, "test").unwrap().unwrap();
		let series = catalog.find_series_by_name(&mut tx, namespace.id(), "s").unwrap().unwrap();
		let metadata = catalog.find_series_metadata(&mut tx, series.id, Partition::default()).unwrap().unwrap();
		txn.rollback().unwrap();
		metadata
	}

	fn race_unchecked_bulks(mark: impl Fn(&SeriesMaterializationActor, &StandardEngine, &Series)) {
		// Every mark write races 20000 unchecked bulks; the counter and count must still equal the stored rows.
		let test = TestEngine::new();
		test.admin("CREATE NAMESPACE test");
		test.admin("CREATE SERIES test::s { k: int8, v: int4 } WITH { key: k }");
		test.command("INSERT test::s [{ k: 0, v: 0 }]");
		let engine = test.inner().clone();
		let store = ColumnStore::memory().unwrap();
		let compressor = Compressor::new(store.session().clone());
		let actor = SeriesMaterializationActor::new(
			engine.clone(),
			store,
			compressor,
			BlockEncodings::empty(),
			Duration::from_milliseconds(50).unwrap(),
			1_000,
			Duration::from_milliseconds(0).unwrap(),
			1,
			Duration::from_seconds(60).unwrap(),
		);
		let series = {
			let catalog = engine.catalog();
			let mut txn = engine.begin_command(IdentityId::system()).unwrap();
			let mut tx = Transaction::Command(&mut txn);
			let namespace = catalog.find_namespace_by_name(&mut tx, "test").unwrap().unwrap();
			let series = catalog.find_series_by_name(&mut tx, namespace.id(), "s").unwrap().unwrap();
			txn.rollback().unwrap();
			series
		};

		scope(|scope| {
			let bulk = scope.spawn(|| {
				for k in 1..=20_000i64 {
					let mut builder = engine.bulk_insert_unchecked(IdentityId::system());
					builder.series("test::s").rows([params! { k: k, v: 1 }]).done();
					builder.execute().expect("the unchecked bulk commits");
				}
			});
			while !bulk.is_finished() {
				mark(&actor, &engine, &series);
			}
			bulk.join().expect("the bulk writer never fails");
		});

		let stored = TestEngine::row_count(&test.query("FROM test::s")) as u64;
		let metadata = stored_metadata(&engine);
		assert_eq!(metadata.row_count, stored, "row_count must count every committed row");
		assert_eq!(
			metadata.sequence_counter, stored,
			"the counter must never rewind below the rows it numbered"
		);
	}

	#[test]
	fn a_dirty_mark_reset_never_rewinds_what_an_unchecked_bulk_committed() {
		// A reset must clear the latest dirty range, otherwise it writes back a stale counter and count.
		race_unchecked_bulks(|actor, _, series| {
			actor.reset_dirty_mark(series, Partition::default()).expect("the reset commits");
		});
	}

	#[test]
	fn a_dirty_mark_clear_never_rewinds_what_an_unchecked_bulk_committed() {
		// A clear must judge the latest dirty range, otherwise it writes back a stale counter and count.
		// Without counting clears that wrote, the equality check could skip every one and pass vacuously.
		let cleared = Cell::new(0u64);
		race_unchecked_bulks(|actor, engine, series| {
			let observed = stored_metadata(engine);
			actor.clear_dirty_mark(series, Partition::default(), &observed).expect("the clear commits");
			let after = stored_metadata(engine);
			if observed.dirty_from_key < observed.dirty_to_key
				&& after.dirty_from_key == u64::MAX
				&& after.dirty_to_key == 0
			{
				cleared.set(cleared.get() + 1);
			}
		});
		assert!(cleared.get() > 0, "no clear wrote during the race, so nothing was tested");
	}

	#[test]
	fn a_conflicting_seal_is_skipped_until_the_next_tick() {
		// A conflict with a user write must not panic the actor; the dirty mark survives and the next tick
		// redoes it.
		assert!(skip_on_conflict(Err(TransactionError::Conflict.into())).is_ok());
	}

	#[test]
	fn any_other_seal_error_still_reaches_the_panic() {
		// Only the conflict is expected; swallowing any other error would keep the actor running on wrong data.
		let err = skip_on_conflict(Err(TransactionError::RolledBack.into())).unwrap_err();
		assert_eq!(err.code, "TXN_002");
	}

	fn scanned() -> SeriesPartitionMetadata {
		SeriesPartitionMetadata {
			row_count: 35,
			oldest_key: 4,
			newest_key: 88,
			dirty_from_key: 4,
			dirty_to_key: 89,
			..SeriesPartitionMetadata::new()
		}
	}

	#[test]
	fn a_delete_inside_the_dirty_range_after_the_scan_keeps_the_dirty_mark() {
		// The block was built from the pre-delete snapshot; clearing the mark would bring the deleted row back.
		let mut current = scanned();
		current.row_count -= 1;
		assert!(!unchanged_since_scan(&current, &scanned()));
	}

	#[test]
	fn a_partition_untouched_since_the_scan_clears_its_dirty_mark() {
		// Without this the mark never clears and every tick rebuilds the same buckets.
		assert!(unchanged_since_scan(&scanned(), &scanned()));
	}
}
