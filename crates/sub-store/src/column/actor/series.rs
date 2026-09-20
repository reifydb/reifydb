// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::{
	catalog::Catalog,
	store::column_snapshot::{create::ColumnSnapshotToCreate, update::ColumnSnapshotToUpdate},
};
use reifydb_column::{
	bucket::{Bucket, BucketId, bucket_for, is_closed},
	compress::Compressor,
	snapshot::ColumnBlock,
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			column_snapshot::ColumnSnapshotSource,
			id::SeriesId,
			series::{Series, SeriesPartitionMetadata},
		},
		resolved::{ResolvedNamespace, ResolvedSeries},
	},
	value::column::columns::Columns,
};
use reifydb_engine::{
	engine::StandardEngine,
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
use reifydb_store_column::ColumnStore;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction};
use reifydb_value::{
	Result,
	fragment::Fragment,
	params::Params,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId, partition::Partition, value_type::ValueType},
};
use tracing::debug;

use crate::column::{
	actor::{
		SeriesMessage,
		batches::{column_block_from_batches, system_column_schema},
	},
	error::SubStoreError,
};

pub struct SeriesBucketState {
	pub materialized_at_sequence: u64,
}

pub struct SeriesMaterializationState {
	pub bucket_state: HashMap<(SeriesId, BucketId), SeriesBucketState>,
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
		let Some(metadata) = catalog.find_series_metadata(
			&mut Transaction::Query(&mut *query_txn),
			series.id,
			Partition::default(),
		)?
		else {
			return Ok(());
		};
		if metadata.row_count == 0 {
			return Ok(());
		}
		let first = bucket_for(metadata.oldest_key, self.bucket_width);
		let last = bucket_for(metadata.newest_key, self.bucket_width);
		let mut start = first.start;
		while start <= last.start {
			let bucket = Bucket {
				start,
				end: start + self.bucket_width,
				width: self.bucket_width,
			};
			start = start.saturating_add(self.bucket_width);
			self.maybe_materialize_bucket(state, query_txn, series, &metadata, &bucket, now_wall)?;
		}
		Ok(())
	}

	fn maybe_materialize_bucket(
		&self,
		state: &mut SeriesMaterializationState,
		query_txn: &mut QueryTransaction,
		series: &Series,
		metadata: &SeriesPartitionMetadata,
		bucket: &Bucket,
		now_wall: DateTime,
	) -> Result<()> {
		if !is_closed(bucket, series, metadata, now_wall, self.grace) {
			return Ok(());
		}
		let key = (series.id, bucket.id());
		let need_remat = match state.bucket_state.get(&key) {
			None => true,
			Some(s) => s.materialized_at_sequence < metadata.sequence_counter,
		};
		if !need_remat {
			return Ok(());
		}
		self.materialize_bucket(query_txn, series, metadata, bucket)?;
		state.bucket_state.insert(
			key,
			SeriesBucketState {
				materialized_at_sequence: metadata.sequence_counter,
			},
		);
		Ok(())
	}

	fn materialize_bucket(
		&self,
		query_txn: &mut QueryTransaction,
		series: &Series,
		metadata: &SeriesPartitionMetadata,
		bucket: &Bucket,
	) -> Result<()> {
		let sealed_at_commit_version = query_txn.version();
		let resolved_series = self.resolve_series_target(query_txn, series)?;
		let batches = self.scan_bucket_batches(query_txn, resolved_series, bucket)?;

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
		self.upsert_snapshot_and_store(series, metadata, bucket, sealed_at_commit_version, block)
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

		let mut scan = SeriesScanNode::new(
			resolved_series,
			Some(bucket.start),
			Some(bucket.end),
			None,
			None,
			Arc::clone(&context),
		)?;

		let mut tx: Transaction<'_> = query_txn.into();
		scan.initialize(&mut tx, &context)?;
		let mut ctx = (*context).clone();
		let mut batches = Vec::new();
		while let Some(batch) = scan.next(&mut tx, &mut ctx)? {
			batches.push(batch);
		}
		Ok(batches)
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
		metadata: &SeriesPartitionMetadata,
		bucket: &Bucket,
		sealed_at_commit_version: CommitVersion,
		block: Arc<ColumnBlock>,
	) -> Result<()> {
		let row_count = block.len() as u64;
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let cat = self.engine.catalog();
		let column_snapshot = match cat.find_column_snapshot_for_series_bucket(
			&mut Transaction::Admin(&mut admin),
			series.id,
			None,
			bucket.start,
		)? {
			Some(existing) => cat.update_column_snapshot(
				&mut admin,
				existing.id,
				ColumnSnapshotToUpdate {
					sequence_counter: metadata.sequence_counter,
					read_version: sealed_at_commit_version,
					row_count,
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
						partition: None,
						sequence_counter: metadata.sequence_counter,
						sealed_at_commit_version,
					},
					row_count,
					partition_values: Vec::new(),
					stats: Vec::new(),
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
			bucket_state: HashMap::new(),
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
