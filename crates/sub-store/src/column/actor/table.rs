// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::store::column_snapshot::create::ColumnSnapshotToCreate;
use reifydb_column::{
	compress::Compressor,
	snapshot::{ColumnBlock, SystemColumn},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{column_snapshot::ColumnSnapshotSource, id::TableId, table::Table},
	value::column::columns::Columns,
};
use reifydb_engine::{
	engine::StandardEngine,
	vm::{
		stack::SymbolTable,
		volcano::{
			query::{QueryContext, QueryNode, query_budget},
			scan::table::TableScanNode,
		},
	},
};
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	timers::TimerHandle,
	traits::{Actor, Directive},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction};
use reifydb_value::{
	Result,
	params::Params,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId, value_type::ValueType},
};
use tracing::{debug, warn};

use crate::column::{
	actor::{TableMessage, batches::column_block_from_batches},
	block_store::ColumnBlockStore,
};

pub struct TableMaterializationState {
	pub last_seen: HashMap<TableId, CommitVersion>,
	_timer_handle: Option<TimerHandle>,
}

pub struct TableMaterializationActor {
	engine: StandardEngine,
	block_store: ColumnBlockStore,
	compressor: Compressor,
	tick_interval: Duration,
}

impl TableMaterializationActor {
	pub fn new(
		engine: StandardEngine,
		block_store: ColumnBlockStore,
		compressor: Compressor,
		tick_interval: Duration,
	) -> Self {
		Self {
			engine,
			block_store,
			compressor,
			tick_interval,
		}
	}

	pub fn block_store(&self) -> &ColumnBlockStore {
		&self.block_store
	}

	fn run_tick(&self, state: &mut TableMaterializationState, _now: DateTime) {
		let Some(mut query_txn) = self.begin_query_or_warn() else {
			return;
		};
		let current = query_txn.version();
		let Some(tables) = self.list_tables_or_warn(&mut query_txn) else {
			return;
		};
		for table in tables {
			self.materialize_unseen_table(state, &mut query_txn, &table, current);
		}
	}

	#[inline]
	fn begin_query_or_warn(&self) -> Option<QueryTransaction> {
		match self.engine.begin_query(IdentityId::system()) {
			Ok(t) => Some(t),
			Err(e) => {
				warn!("table materialization: begin_query failed: {e}");
				None
			}
		}
	}

	#[inline]
	fn list_tables_or_warn(&self, query_txn: &mut QueryTransaction) -> Option<Vec<Table>> {
		match self.engine.catalog().list_tables_all(&mut Transaction::Query(query_txn)) {
			Ok(t) => Some(t),
			Err(e) => {
				warn!("table materialization: list_tables_all failed: {e}");
				None
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
	) {
		if state.last_seen.get(&table.id).copied() == Some(current) {
			return;
		}
		match self.materialize_table(query_txn, table, current) {
			Ok(()) => {
				state.last_seen.insert(table.id, current);
			}
			Err(e) => {
				warn!("table materialization skipped for {:?}: {e}", table.id);
			}
		}
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
		let block_arc = Arc::new(self.build_column_block(table, batches)?);
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
	fn build_column_block(&self, table: &Table, batches: Vec<Columns>) -> Result<ColumnBlock> {
		let mut schema: Vec<(String, ValueType)> =
			table.columns.iter().map(|c| (c.name.clone(), c.constraint.get_type())).collect();
		for sc in SystemColumn::ALL {
			schema.push((sc.name().to_string(), sc.ty()));
		}
		column_block_from_batches(schema, batches, &self.compressor)
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
