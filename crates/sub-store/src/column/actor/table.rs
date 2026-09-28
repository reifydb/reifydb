// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use reifydb_catalog::store::column_snapshot::create::ColumnSnapshotToCreate;
use reifydb_column::{compress::Compressor, snapshot::ColumnBlock};
use reifydb_core::{
	common::CommitVersion,
	event::{EventListener, transaction::PostCommitEvent},
	interface::catalog::{column_snapshot::ColumnSnapshotSource, id::TableId, storage::StorageId, table::Table},
	key::{
		any::TaggedKey,
		row::{PartitionedRowKey, RowKey},
	},
	value::column::columns::Columns,
};
use reifydb_engine::{
	engine::StandardEngine,
	vm::volcano::{
		query::{QueryContext, QueryNode, query_budget},
		scan::table::TableScanNode,
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
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction};
use reifydb_value::{
	Result,
	params::Params,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId, value_type::ValueType},
};
use tracing::debug;

use crate::column::actor::{
	TableMessage,
	batches::{column_block_from_batches, system_column_schema},
};

#[derive(Clone, Default)]
pub struct TableChanges {
	versions: Arc<DashMap<TableId, CommitVersion>>,
}

impl TableChanges {
	pub fn new() -> Self {
		Self::default()
	}

	fn record(&self, table: TableId, version: CommitVersion) {
		self.versions.entry(table).and_modify(|changed| *changed = (*changed).max(version)).or_insert(version);
	}

	fn changed_at(&self, table: TableId) -> Option<CommitVersion> {
		self.versions.get(&table).map(|changed| *changed)
	}
}

impl EventListener<PostCommitEvent> for TableChanges {
	fn on(&self, event: &PostCommitEvent) {
		let version = event.version().commit;
		for delta in event.deltas().iter() {
			if let Some(table) = changed_table(delta.key()) {
				self.record(table, version);
			}
		}
	}
}

fn changed_table(key: &TaggedKey) -> Option<TableId> {
	match key {
		TaggedKey::Row(RowKey {
			storage: StorageId::Table(table),
			..
		})
		| TaggedKey::PartitionedRow(PartitionedRowKey {
			storage: StorageId::Table(table),
			..
		}) => Some(*table),
		_ => None,
	}
}

pub struct TableMaterializationState {
	pub last_seen: HashMap<TableId, CommitVersion>,
	_timer_handle: Option<TimerHandle>,
}

pub struct TableMaterializationActor {
	engine: StandardEngine,
	block_store: ColumnStore,
	compressor: Compressor,
	tick_interval: Duration,
	changes: TableChanges,
}

impl TableMaterializationActor {
	pub fn new(
		engine: StandardEngine,
		block_store: ColumnStore,
		compressor: Compressor,
		tick_interval: Duration,
		changes: TableChanges,
	) -> Self {
		Self {
			engine,
			block_store,
			compressor,
			tick_interval,
			changes,
		}
	}

	pub fn block_store(&self) -> &ColumnStore {
		&self.block_store
	}

	fn run_tick(&self, state: &mut TableMaterializationState, _now: DateTime) {
		let mut query_txn = match self.engine.begin_query(IdentityId::system()) {
			Ok(txn) => txn,
			Err(e) => panic!("table materialization: begin_query failed: {e}"),
		};
		let current = query_txn.version();
		let tables = match self.engine.catalog().list_tables(&mut Transaction::Query(&mut query_txn)) {
			Ok(tables) => tables,
			Err(e) => panic!("table materialization: list_tables failed: {e}"),
		};
		for table in tables {
			if let Err(e) = self.materialize_unseen_table(state, &mut query_txn, &table, current) {
				panic!("table materialization failed for table {:?} ({}): {e}", table.id, table.name);
			}
		}
	}

	#[inline]
	fn materialize_unseen_table(
		&self,
		state: &mut TableMaterializationState,
		query_txn: &mut QueryTransaction,
		table: &Table,
		current: CommitVersion,
	) -> Result<()> {
		if let Some(materialized) = state.last_seen.get(&table.id).copied() {
			let changed = self.changes.changed_at(table.id);
			if materialized == current || changed.is_none_or(|changed| changed <= materialized) {
				return Ok(());
			}
		}
		self.materialize_table(query_txn, table, current)?;
		state.last_seen.insert(table.id, current);
		Ok(())
	}

	fn materialize_table(
		&self,
		query_txn: &mut QueryTransaction,
		table: &Table,
		version: CommitVersion,
	) -> Result<()> {
		reifydb_assertions! {
			let scan_version = query_txn.version();
			assert!(
				scan_version == version,
				"table materialization scans at query version {} but records the column snapshot under commit_version {}; a mismatch makes the snapshot metadata claim a version the materialized rows do not reflect, so later reads resolve the wrong block for table {:?}",
				scan_version,
				version,
				table.id
			);
		}
		let context = self.build_query_context();
		let batches = self.scan_table_batches(query_txn, table, &context)?;
		let block_arc = Arc::new(self.build_column_block(table, batches, version)?);
		self.store_table_snapshot(table, version, block_arc)
	}

	#[inline]
	fn build_query_context(&self) -> Arc<QueryContext> {
		let services = self.engine.services();
		let memory = query_budget(&services);
		Arc::new(QueryContext {
			services,
			source: None,
			batch_size: 1024,
			params: Params::None,
			symbols: SymbolTable::new(),
			identity: IdentityId::system(),
			memory,
		})
	}

	#[inline]
	fn scan_table_batches(
		&self,
		query_txn: &mut QueryTransaction,
		table: &Table,
		context: &Arc<QueryContext>,
	) -> Result<Vec<Columns>> {
		let mut tx: Transaction<'_> = query_txn.into();
		let resolved = self.engine.catalog().resolve_table(&mut tx, table.id)?;
		let mut scan = TableScanNode::new(resolved, None, Arc::clone(context), &mut tx)?;
		scan.initialize(&mut tx, context)?;
		let mut ctx = (**context).clone();
		let mut batches = Vec::new();
		while let Some(batch) = scan.next(&mut tx, &mut ctx)? {
			batches.push(batch);
		}
		Ok(batches)
	}

	#[inline]
	fn build_column_block(
		&self,
		table: &Table,
		batches: Vec<Columns>,
		version: CommitVersion,
	) -> Result<ColumnBlock> {
		let mut schema: Vec<(String, ValueType)> =
			table.columns.iter().map(|c| (c.name.clone(), c.constraint.get_type())).collect();
		schema.extend(system_column_schema(&table.time, !table.partition_by.is_empty()));
		column_block_from_batches(schema, batches, version, &self.compressor)
	}

	#[inline]
	fn store_table_snapshot(
		&self,
		table: &Table,
		version: CommitVersion,
		block_arc: Arc<ColumnBlock>,
	) -> Result<()> {
		let row_count = block_arc.len() as u64;
		let mut admin = self.engine.begin_admin(IdentityId::system())?;
		let column_snapshot = self.engine.catalog().create_column_snapshot(
			&mut admin,
			ColumnSnapshotToCreate {
				namespace: table.namespace,
				source: ColumnSnapshotSource::Table {
					table_id: table.id,
					commit_version: version,
				},
				row_count,
				partition_values: Vec::new(),
				stats: Vec::new(),
			},
		)?;
		self.block_store.persist(column_snapshot.id, block_arc.as_ref())?;
		commit_admin(admin)?;
		self.block_store.put(column_snapshot.id, block_arc);
		Ok(())
	}
}

fn commit_admin(mut admin: AdminTransaction) -> Result<()> {
	admin.commit()?;
	Ok(())
}

impl Actor for TableMaterializationActor {
	type State = TableMaterializationState;
	type Message = TableMessage;

	fn init(&self, ctx: &Context<TableMessage>) -> TableMaterializationState {
		debug!("TableMaterializationActor started (tick interval = {:?})", self.tick_interval);
		let handle =
			ctx.schedule_tick(self.tick_interval, |nanos| TableMessage::Tick(DateTime::from_nanos(nanos)));
		TableMaterializationState {
			last_seen: HashMap::new(),
			_timer_handle: Some(handle),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		if ctx.is_cancelled() {
			return Directive::Stop;
		}
		match msg {
			TableMessage::Tick(now) => self.run_tick(state, now),
			TableMessage::Shutdown => {
				debug!("TableMaterializationActor shutting down");
				return Directive::Stop;
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("TableMaterializationActor stopped");
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(64)
	}
}
