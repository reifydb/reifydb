// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc, time::Duration};

use reifydb_catalog::store::column_snapshot::create::ColumnSnapshotToCreate;
use reifydb_column::{compress::Compressor, snapshot::SystemColumn};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{column_snapshot::ColumnSnapshotSource, id::TableId, table::Table},
};
use reifydb_engine::{
	engine::StandardEngine,
	vm::{
		stack::SymbolTable,
		volcano::{
			query::{QueryContext, QueryNode},
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
use reifydb_type::{
	Result,
	params::Params,
	value::{datetime::DateTime, identity::IdentityId, r#type::Type},
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
		let mut query_txn = match self.engine.begin_query(IdentityId::system()) {
			Ok(t) => t,
			Err(e) => {
				warn!("table materialization: begin_query failed: {e}");
				return;
			}
		};
		let current = query_txn.version();

		let tables = match self.engine.catalog().list_tables_all(&mut Transaction::Query(&mut query_txn)) {
			Ok(t) => t,
			Err(e) => {
				warn!("table materialization: list_tables_all failed: {e}");
				return;
			}
		};
		for table in tables {
			if state.last_seen.get(&table.id).copied() == Some(current) {
				continue;
			}
			match self.materialize_table(&mut query_txn, &table, current) {
				Ok(()) => {
					state.last_seen.insert(table.id, current);
				}
				Err(e) => {
					warn!("table materialization skipped for {:?}: {e}", table.id);
				}
			}
		}
	}

	fn materialize_table(
		&self,
		query_txn: &mut QueryTransaction,
		table: &Table,
		version: CommitVersion,
	) -> Result<()> {
		let services = self.engine.services();
		let catalog = self.engine.catalog();
		let mut tx: Transaction<'_> = (&mut *query_txn).into();
		let resolved = catalog.resolve_table(&mut tx, table.id)?;

		let context = Arc::new(QueryContext {
			services,
			source: None,
			batch_size: 1024,
			params: Params::None,
			symbols: SymbolTable::new(),
			identity: IdentityId::system(),
		});

		let mut scan = TableScanNode::new(resolved, Arc::clone(&context), &mut tx)?;
		scan.initialize(&mut tx, &context)?;
		let mut ctx = (*context).clone();
		let mut batches = Vec::new();
		while let Some(batch) = scan.next(&mut tx, &mut ctx)? {
			batches.push(batch);
		}

		let mut schema: Vec<(String, Type)> =
			table.columns.iter().map(|c| (c.name.clone(), c.constraint.get_type())).collect();
		for sc in SystemColumn::ALL {
			schema.push((sc.name().to_string(), sc.ty()));
		}
		let block = column_block_from_batches(schema, batches, &self.compressor)?;
		let row_count = block.len() as u64;
		let block_arc = Arc::new(block);

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
