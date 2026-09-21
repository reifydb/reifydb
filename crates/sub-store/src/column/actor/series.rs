// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use reifydb_catalog::{
	catalog::Catalog,
	store::column_snapshot::{create::ColumnSnapshotToCreate, update::ColumnSnapshotToUpdate},
};
use reifydb_column::{
	bucket::{Bucket, BucketId, bucket_for, is_closed},
	compress::Compressor,
	snapshot::ColumnBlock,
	stats::block_stats,
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			column_snapshot::{ColumnSnapshotSource, ColumnStats},
			id::{ColumnSnapshotId, SeriesId},
			object::ObjectId,
			series::{Series, SeriesPartitionMetadata},
		},
		resolved::{ResolvedNamespace, ResolvedSeries},
	},
	key::{any::TaggedKey, partition::PartitionKey},
	value::column::columns::Columns,
};
use reifydb_engine::{
	engine::StandardEngine,
	partition::decode_partition_values,
	vm::volcano::{
		query::{QueryContext, QueryNode, query_budget},
		scan::series::SeriesScanNode,
	},
};
use reifydb_evaluate::stack::SymbolTable;
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	timers::TimerHandle,
	traits::{Actor, Directive},
};
use reifydb_store_column::store::ColumnStore;
use reifydb_transaction::{
	multi::RangeScope,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	params::Params,
	reifydb_assertions,
	value::{
		Value, datetime::DateTime, duration::Duration, identity::IdentityId, partition::Partition,
		value_type::ValueType,
	},
};
use tracing::{debug, warn};

const PARTITION_CARDINALITY_WARN: usize = 1000;

use crate::column::{
	actor::{
		SeriesMessage,
		batches::{column_block_from_batches, system_column_schema},
	},
	error::SubStoreError,
};

pub struct SeriesMaterializationState {
	pub bucket_state: HashSet<(SeriesId, Partition, BucketId)>,
	pub partitions: HashMap<SeriesId, Vec<(Partition, Vec<Value>)>>,
	_timer_handle: Option<TimerHandle>,
}

pub struct SeriesMaterializationActor {
	engine: StandardEngine,
	block_store: ColumnStore,
	compressor: Compressor,
	tick_interval: Duration,
	bucket_width: u64,
	grace: Duration,
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
		tick_interval: Duration,
		bucket_width: u64,
		grace: Duration,
	) -> Self {
		Self {
			engine,
			block_store,
			compressor,
			tick_interval,
			bucket_width,
			grace,
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
		state.partitions.clear();
		for series in series_list {
			if let Err(e) =
				self.materialize_series_buckets(state, &mut query_txn, &catalog, &series, now_wall)
			{
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
				self.drop_partition_snapshots(state, series, partition)?;
				continue;
			}
			let first = bucket_for(metadata.oldest_key, self.bucket_width);
			let last = bucket_for(metadata.newest_key, self.bucket_width);
			let mut start = first.start;
			let mut deferred = false;
			let scope = PartitionScope {
				partition,
				values: &partition_values,
				metadata: &metadata,
			};
			while start <= last.start {
				let bucket = Bucket {
					start,
					end: start + self.bucket_width,
					width: self.bucket_width,
				};
				start = start.saturating_add(self.bucket_width);
				deferred |= self
					.maybe_materialize_bucket(state, query_txn, series, scope, &bucket, now_wall)?;
			}
			if !deferred {
				self.clear_dirty_mark(
					series,
					partition,
					metadata.dirty_from_key,
					metadata.dirty_to_key,
				)?;
			}
		}
		Ok(())
	}

	fn drop_partition_snapshots(
		&self,
		state: &mut SeriesMaterializationState,
		series: &Series,
		partition: Partition,
	) -> Result<()> {
		let stored_partition = if series.partition_by.is_empty() {
			None
		} else {
			Some(partition)
		};
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let catalog = self.engine.catalog();
		let stale: Vec<ColumnSnapshotId> = catalog
			.list_column_snapshots_for_series(&mut Transaction::Admin(&mut admin), series.id)?
			.into_iter()
			.filter(|snapshot| match snapshot.source {
				ColumnSnapshotSource::SeriesBucket {
					partition,
					..
				} => partition == stored_partition,
				_ => false,
			})
			.map(|snapshot| snapshot.id)
			.collect();
		if stale.is_empty() {
			return Ok(());
		}
		for id in &stale {
			catalog.drop_column_snapshot(&mut admin, *id)?;
		}
		commit_admin(admin)?;
		for id in &stale {
			self.block_store.remove(*id)?;
		}
		state.bucket_state.retain(|(id, part, _)| *id != series.id || *part != partition);
		self.reset_dirty_mark(series, partition)
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
		catalog.update_series_metadata_txn(&mut tx, series.id, partition, metadata)?;
		commit_admin(admin)
	}

	fn clear_dirty_mark(
		&self,
		series: &Series,
		partition: Partition,
		observed_from: u64,
		observed_to: u64,
	) -> Result<()> {
		if observed_from >= observed_to {
			return Ok(());
		}
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let catalog = self.engine.catalog();
		let mut tx = Transaction::Admin(&mut admin);
		let Some(mut metadata) = catalog.find_series_metadata(&mut tx, series.id, partition)? else {
			return Ok(());
		};
		if metadata.dirty_from_key < observed_from || metadata.dirty_to_key > observed_to {
			return Ok(());
		}
		metadata.dirty_from_key = u64::MAX;
		metadata.dirty_to_key = 0;
		catalog.update_series_metadata_txn(&mut tx, series.id, partition, metadata)?;
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
		if partitions.len() > PARTITION_CARDINALITY_WARN {
			warn!(
				series = %series.name,
				series_id = ?series.id,
				partitions = partitions.len(),
				"series exceeds {} partitions; the column store writes one block per bucket per partition",
				PARTITION_CARDINALITY_WARN
			);
		}
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
				partitions.push((key.partition, decode_partition_values(&entry.bytes)));
			}
		}
		Ok(partitions)
	}

	fn maybe_materialize_bucket(
		&self,
		state: &mut SeriesMaterializationState,
		query_txn: &mut QueryTransaction,
		series: &Series,
		scope: PartitionScope<'_>,
		bucket: &Bucket,
		now_wall: DateTime,
	) -> Result<bool> {
		let key = (series.id, scope.partition, bucket.id());
		let built = state.bucket_state.contains(&key);
		let dirty = bucket.start < scope.metadata.dirty_to_key && bucket.end > scope.metadata.dirty_from_key;
		if !is_closed(bucket, series, scope.metadata, now_wall, self.grace) {
			return Ok(built && dirty);
		}
		if built && !dirty {
			return Ok(false);
		}
		self.materialize_bucket(query_txn, series, scope, bucket)?;
		state.bucket_state.insert(key);
		Ok(false)
	}

	fn materialize_bucket(
		&self,
		query_txn: &mut QueryTransaction,
		series: &Series,
		scope: PartitionScope<'_>,
		bucket: &Bucket,
	) -> Result<()> {
		let sealed_at_commit_version = query_txn.version();
		let resolved_series = self.resolve_series_target(query_txn, series)?;
		let batches = self.scan_bucket_batches(query_txn, resolved_series, scope.partition, series, bucket)?;

		reifydb_assertions! {
			let after_scan = query_txn.version();
			assert!(
				sealed_at_commit_version == after_scan,
				"query snapshot version moved during the bucket scan, so the snapshot would record a \
				 sealed_at_commit_version that the scanned rows were not actually read at; a time-travel \
				 reader of the snapshot would then see data inconsistent with its recorded read_version \
				 (captured before scan={sealed_at_commit_version:?}, observed after scan={after_scan:?})"
			);
		}

		let block = Arc::new(self.build_column_block(series, batches, sealed_at_commit_version)?);
		let stats = block_stats(block.as_ref())?;
		self.upsert_snapshot_and_store(series, scope, &stats, bucket, sealed_at_commit_version, block)
	}

	#[inline]
	fn resolve_series_target(&self, query_txn: &mut QueryTransaction, series: &Series) -> Result<ResolvedSeries> {
		let catalog = self.engine.catalog();
		let namespace_def = catalog
			.find_namespace(&mut Transaction::Query(query_txn), series.namespace)?
			.ok_or_else(|| missing_namespace(series))?;
		let resolved_namespace =
			ResolvedNamespace::new(Fragment::internal(namespace_def.name()), namespace_def.clone());
		Ok(ResolvedSeries::new(Fragment::internal(series.name.clone()), resolved_namespace, series.clone()))
	}

	#[inline]
	fn scan_bucket_batches(
		&self,
		query_txn: &mut QueryTransaction,
		resolved_series: ResolvedSeries,
		partition: Partition,
		series: &Series,
		bucket: &Bucket,
	) -> Result<Vec<Columns>> {
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
		let tags = self.bucket_scan_tags(query_txn, series)?;

		let mut tx: Transaction<'_> = query_txn.into();
		let mut ctx = (*context).clone();
		let mut batches = Vec::new();
		for tag in tags {
			let mut scan = SeriesScanNode::new(
				resolved_series.clone(),
				Some(bucket.start),
				Some(bucket.end),
				tag,
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
	fn bucket_scan_tags(&self, query_txn: &mut QueryTransaction, series: &Series) -> Result<Vec<Option<u8>>> {
		let Some(sumtype) = series.tag else {
			return Ok(vec![None]);
		};
		let definition =
			self.engine.catalog().find_sumtype(&mut Transaction::Query(query_txn), sumtype)?.ok_or(
				SubStoreError::SumTypeMissing {
					sumtype,
					series: series.id,
				},
			)?;
		let mut tags: Vec<u8> = definition.variants.iter().map(|variant| variant.tag).collect();
		tags.sort_unstable_by(|left, right| right.cmp(left));
		Ok(tags.into_iter().map(Some).collect())
	}

	#[inline]
	fn build_column_block(
		&self,
		series: &Series,
		batches: Vec<Columns>,
		version: CommitVersion,
	) -> Result<ColumnBlock> {
		let schema = scan_output_schema(series);
		column_block_from_batches(schema, batches, version, &self.compressor)
	}

	#[inline]
	fn upsert_snapshot_and_store(
		&self,
		series: &Series,
		scope: PartitionScope<'_>,
		stats: &[ColumnStats],
		bucket: &Bucket,
		sealed_at_commit_version: CommitVersion,
		block: Arc<ColumnBlock>,
	) -> Result<()> {
		let row_count = block.len() as u64;
		let stored_partition = if series.partition_by.is_empty() {
			None
		} else {
			Some(scope.partition)
		};
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let cat = self.engine.catalog();
		let column_snapshot = match cat.find_column_snapshot_for_series_bucket(
			&mut Transaction::Admin(&mut admin),
			series.id,
			stored_partition,
			bucket.start,
		)? {
			Some(existing) => cat.update_column_snapshot(
				&mut admin,
				existing.id,
				ColumnSnapshotToUpdate {
					sequence_counter: scope.metadata.sequence_counter,
					read_version: sealed_at_commit_version,
					row_count,
					partition_values: scope.values.to_vec(),
					stats: stats.to_vec(),
				},
			)?,
			None => cat.create_column_snapshot(
				&mut admin,
				ColumnSnapshotToCreate {
					namespace: series.namespace,
					source: ColumnSnapshotSource::SeriesBucket {
						series_id: series.id,
						bucket_start: bucket.start,
						bucket_width: bucket.width,
						partition: stored_partition,
						sequence_counter: scope.metadata.sequence_counter,
						sealed_at_commit_version,
					},
					row_count,
					partition_values: scope.values.to_vec(),
					stats: stats.to_vec(),
				},
			)?,
		};
		self.block_store.persist(column_snapshot.id, block.as_ref())?;
		commit_admin(admin)?;
		self.block_store.put(column_snapshot.id, block);
		Ok(())
	}
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

	let system = system_column_schema(&series.time);
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
			bucket_state: HashSet::new(),
			partitions: HashMap::new(),
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
