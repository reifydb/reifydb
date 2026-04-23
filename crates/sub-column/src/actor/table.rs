// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc, time::Duration};

use reifydb_column::{
	compress::Compressor,
	registry::SnapshotRegistry,
	snapshot::{Snapshot, SnapshotId, SnapshotSource},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::TableId, table::Table},
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
use reifydb_transaction::transaction::{Transaction, query::QueryTransaction};
use reifydb_type::{
	Result,
	params::Params,
	value::{datetime::DateTime, identity::IdentityId},
};
use tracing::{debug, warn};

use crate::actor::{TableMessage, batches::column_block_from_batches};

pub struct TableMaterializationState {
	pub last_seen: HashMap<TableId, CommitVersion>,
	_timer_handle: Option<TimerHandle>,
}

// Periodic per-table materialization. Each tick: open a fresh read transaction,
// walk the catalog, skip tables whose `CommitVersion` hasn't advanced, and
// otherwise drive the engine's `TableScanNode` to collect `Columns` batches,
// concatenate into a single-chunk `ColumnBlock`, wrap as a `Snapshot`, and
// insert into the shared `SnapshotRegistry`.
pub struct TableMaterializationActor {
	engine: StandardEngine,
	registry: SnapshotRegistry,
	compressor: Compressor,
	tick_interval: Duration,
}

impl TableMaterializationActor {
	pub fn new(
		engine: StandardEngine,
		registry: SnapshotRegistry,
		compressor: Compressor,
		tick_interval: Duration,
	) -> Self {
		Self {
			engine,
			registry,
			compressor,
			tick_interval,
		}
	}

	pub fn registry(&self) -> &SnapshotRegistry {
		&self.registry
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

		let tables = self.engine.catalog().materialized.list_tables();
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

		let schema: Vec<_> = table.columns.iter().map(|c| (c.name.clone(), c.constraint.get_type())).collect();
		let block = column_block_from_batches(schema, batches, &self.compressor)?;

		let namespace = self
			.engine
			.catalog()
			.materialized
			.find_namespace(table.namespace)
			.map(|ns| ns.name().to_string())
			.unwrap_or_default();

		let snapshot = Snapshot {
			id: SnapshotId::Table {
				table_id: table.id,
				commit_version: version,
			},
			source: SnapshotSource::Table {
				table_id: table.id,
				commit_version: version,
			},
			namespace,
			name: table.name.clone(),
			created_at: self.engine.clock().instant(),
			block,
		};
		self.registry.insert(Arc::new(snapshot));
		Ok(())
	}
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
