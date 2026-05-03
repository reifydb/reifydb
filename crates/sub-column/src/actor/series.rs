// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	sync::Arc,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_column::{
	bucket::{Bucket, BucketId, bucket_for, is_closed},
	compress::Compressor,
	registry::SnapshotRegistry,
	snapshot::{Snapshot, SnapshotId, SnapshotSource},
};
use reifydb_core::interface::{
	catalog::{
		id::SeriesId,
		series::{Series, SeriesMetadata},
	},
	resolved::{ResolvedNamespace, ResolvedSeries},
};
use reifydb_engine::{
	engine::StandardEngine,
	vm::{
		stack::SymbolTable,
		volcano::{
			query::{QueryContext, QueryNode},
			scan::series::SeriesScanNode,
		},
	},
};
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	timers::TimerHandle,
	traits::{Actor, Directive},
};
use reifydb_transaction::transaction::{Transaction, query::QueryTransaction};
use reifydb_type::{
	Result,
	fragment::Fragment,
	params::Params,
	value::{datetime::DateTime, identity::IdentityId, r#type::Type},
};
use tracing::{debug, warn};

use crate::{
	actor::{SeriesMessage, batches::column_block_from_batches},
	error::SubColumnError,
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
	registry: SnapshotRegistry,
	compressor: Compressor,
	tick_interval: Duration,
	bucket_width: u64,
	grace: Duration,
}

impl SeriesMaterializationActor {
	pub fn new(
		engine: StandardEngine,
		registry: SnapshotRegistry,
		compressor: Compressor,
		tick_interval: Duration,
		bucket_width: u64,
		grace: Duration,
	) -> Self {
		Self {
			engine,
			registry,
			compressor,
			tick_interval,
			bucket_width,
			grace,
		}
	}

	pub fn registry(&self) -> &SnapshotRegistry {
		&self.registry
	}

	fn run_tick(&self, state: &mut SeriesMaterializationState, _now: DateTime) {
		let Some(mut query_txn) = self.begin_query_or_warn() else {
			return;
		};
		let catalog = self.engine.catalog();
		let now_wall = UNIX_EPOCH + Duration::from_nanos(self.engine.clock().now_nanos());
		for series in catalog.materialized().list_series() {
			self.materialize_series_buckets(state, &mut query_txn, &catalog, &series, now_wall);
		}
	}

	#[inline]
	fn begin_query_or_warn(&self) -> Option<QueryTransaction> {
		match self.engine.begin_query(IdentityId::system()) {
			Ok(t) => Some(t),
			Err(e) => {
				warn!("series materialization: begin_query failed: {e}");
				None
			}
		}
	}

	fn materialize_series_buckets(
		&self,
		state: &mut SeriesMaterializationState,
		query_txn: &mut QueryTransaction,
		catalog: &Catalog,
		series: &Series,
		now_wall: SystemTime,
	) {
		let Some(metadata) = self.load_series_metadata_or_warn(query_txn, catalog, series) else {
			return;
		};
		if metadata.row_count == 0 {
			return;
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
			self.maybe_materialize_bucket(state, query_txn, series, &metadata, &bucket, now_wall);
		}
	}

	#[inline]
	fn load_series_metadata_or_warn(
		&self,
		query_txn: &mut QueryTransaction,
		catalog: &Catalog,
		series: &Series,
	) -> Option<SeriesMetadata> {
		let mut tx: Transaction<'_> = query_txn.into();
		match catalog.find_series_metadata(&mut tx, series.id) {
			Ok(Some(m)) => Some(m),
			Ok(None) => None,
			Err(e) => {
				warn!("series materialization: find_series_metadata failed for {:?}: {e}", series.id);
				None
			}
		}
	}

	fn maybe_materialize_bucket(
		&self,
		state: &mut SeriesMaterializationState,
		query_txn: &mut QueryTransaction,
		series: &Series,
		metadata: &SeriesMetadata,
		bucket: &Bucket,
		now_wall: SystemTime,
	) {
		if !is_closed(bucket, series, metadata, now_wall, self.grace) {
			return;
		}
		let key = (series.id, bucket.id());
		let need_remat = match state.bucket_state.get(&key) {
			None => true,
			Some(s) => s.materialized_at_sequence < metadata.sequence_counter,
		};
		if !need_remat {
			return;
		}
		match self.materialize_bucket(query_txn, series, metadata, bucket) {
			Ok(()) => {
				state.bucket_state.insert(
					key,
					SeriesBucketState {
						materialized_at_sequence: metadata.sequence_counter,
					},
				);
			}
			Err(e) => {
				warn!(
					"series materialization skipped for {:?} bucket {:?}: {e}",
					series.id,
					bucket.id()
				);
			}
		}
	}

	fn materialize_bucket(
		&self,
		query_txn: &mut QueryTransaction,
		series: &Series,
		metadata: &SeriesMetadata,
		bucket: &Bucket,
	) -> Result<()> {
		let services = self.engine.services();
		let catalog = self.engine.catalog();

		let namespace_def = catalog
			.materialized()
			.find_namespace(series.namespace)
			.ok_or_else(|| missing_namespace(series))?;
		let resolved_namespace =
			ResolvedNamespace::new(Fragment::internal(namespace_def.name()), namespace_def.clone());
		let resolved_series = ResolvedSeries::new(
			Fragment::internal(series.name.clone()),
			resolved_namespace,
			series.clone(),
		);

		let context = Arc::new(QueryContext {
			services,
			source: None,
			batch_size: 1024,
			params: Params::None,
			symbols: SymbolTable::new(),
			identity: IdentityId::system(),
		});

		let mut scan = SeriesScanNode::new(
			resolved_series,
			Some(bucket.start),
			Some(bucket.end),
			None,
			Arc::clone(&context),
		)?;

		let mut tx: Transaction<'_> = (&mut *query_txn).into();
		scan.initialize(&mut tx, &context)?;
		let mut ctx = (*context).clone();
		let mut batches = Vec::new();
		while let Some(batch) = scan.next(&mut tx, &mut ctx)? {
			batches.push(batch);
		}

		let schema = scan_output_schema(series);
		let block = column_block_from_batches(schema, batches, &self.compressor)?;

		let snapshot = Snapshot {
			id: SnapshotId::Series {
				series_id: series.id,
				bucket: bucket.id(),
			},
			source: SnapshotSource::Series {
				series_id: series.id,
				bucket: *bucket,
				sequence_counter: metadata.sequence_counter,
			},
			namespace: namespace_def.name().to_string(),
			name: series.name.clone(),
			created_at: self.engine.clock().instant(),
			block,
		};
		self.registry.insert(Arc::new(snapshot));
		Ok(())
	}
}

fn scan_output_schema(series: &Series) -> Vec<(String, Type)> {
	let key_name = series.key.column().to_string();
	let key_ty = series
		.columns
		.iter()
		.find(|c| c.name == key_name)
		.map(|c| c.constraint.get_type())
		.unwrap_or(Type::Uint8);

	let mut schema = Vec::with_capacity(series.columns.len() + 1);
	schema.push((key_name.clone(), key_ty));
	if series.tag.is_some() {
		schema.push(("tag".to_string(), Type::Uint1));
	}
	for col in series.data_columns() {
		schema.push((col.name.clone(), col.constraint.get_type()));
	}
	schema
}

fn missing_namespace(series: &Series) -> SubColumnError {
	SubColumnError::NamespaceMissing {
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
