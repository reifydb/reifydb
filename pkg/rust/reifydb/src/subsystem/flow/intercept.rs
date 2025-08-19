// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, Mutex};

use reifydb_core::{
	Result, RowId,
	interceptor::{
		PreCommitContext, PreCommitInterceptor, TablePostDeleteContext,
		TablePostDeleteInterceptor, TablePostInsertContext,
		TablePostInsertInterceptor, TablePostUpdateContext,
		TablePostUpdateInterceptor,
	},
	interface::{TableId, Transaction},
};
use reifydb_engine::StandardEngine;

use crate::ioc::{IocContainer, LazyResolve};

/// Event type for flow processing
#[derive(Debug, Clone)]
pub enum FlowChange {
	Insert {
		table_id: TableId,
		row_id: RowId,
		row: Vec<u8>,
	},
	Update {
		table_id: TableId,
		row_id: RowId,
		before: Vec<u8>,
		after: Vec<u8>,
	},
	Delete {
		table_id: TableId,
		row_id: RowId,
		row: Vec<u8>,
	},
}

pub struct TransactionalFlowInterceptor<T: Transaction> {
	engine: LazyResolve<StandardEngine<T>>,
	ioc: IocContainer,
	// Transaction-scoped change buffer
	changes: Arc<Mutex<Vec<FlowChange>>>,
}

impl<T: Transaction> TransactionalFlowInterceptor<T> {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			engine: LazyResolve::new(),
			ioc,
			changes: Arc::new(Mutex::new(Vec::new())),
		}
	}
}

impl<T: Transaction> Clone for TransactionalFlowInterceptor<T> {
	fn clone(&self) -> Self {
		Self {
			engine: self.engine.clone(),
			ioc: self.ioc.clone(),
			changes: Arc::clone(&self.changes),
		}
	}
}

impl<T: Transaction> TablePostInsertInterceptor<T>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(&self, ctx: &mut TablePostInsertContext<T>) -> Result<()> {
		// Collect insert event
		self.changes.lock().unwrap().push(FlowChange::Insert {
			table_id: ctx.table.id,
			row_id: ctx.id,
			row: ctx.row.to_vec(),
		});
		Ok(())
	}
}

impl<T: Transaction> TablePostUpdateInterceptor<T>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(&self, ctx: &mut TablePostUpdateContext<T>) -> Result<()> {
		// Collect update event
		self.changes.lock().unwrap().push(FlowChange::Update {
			table_id: ctx.table.id,
			row_id: ctx.id,
			before: ctx.old_row.to_vec(),
			after: ctx.row.to_vec(),
		});
		Ok(())
	}
}

impl<T: Transaction> TablePostDeleteInterceptor<T>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(&self, ctx: &mut TablePostDeleteContext<T>) -> Result<()> {
		// Collect delete event
		self.changes.lock().unwrap().push(FlowChange::Delete {
			table_id: ctx.table.id,
			row_id: ctx.id,
			row: ctx.deleted_row.to_vec(),
		});
		Ok(())
	}
}

impl<T: Transaction> PreCommitInterceptor<T>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(&self, _ctx: &mut PreCommitContext<T>) -> Result<()> {
		// Get engine from IoC using lazy resolution (only resolves
		// once)
		let _engine = self.engine.get_or_resolve(&self.ioc)?;

		// Process all collected changes
		let mut changes = self.changes.lock().unwrap();
		for _change in changes.drain(..) {
			println!("{_change:?}")
			// TODO: Process with flow engine
			// This is where you would process the changes through
			// the flow system For now, we just have the
			// infrastructure in place
		}

		Ok(())
	}
}
