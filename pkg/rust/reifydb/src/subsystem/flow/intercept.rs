// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{cell::RefCell, rc::Rc};

use reifydb_core::{
	Result, RowId,
	interceptor::{
		Interceptors, PreCommitContext, PreCommitInterceptor,
		RegisterInterceptor, TablePostDeleteContext,
		TablePostDeleteInterceptor, TablePostInsertContext,
		TablePostInsertInterceptor, TablePostUpdateContext,
		TablePostUpdateInterceptor,
	},
	interface::{TableId, Transaction},
	ioc::{IocContainer, SingleThreadLazyResolve},
};
use reifydb_engine::StandardEngine;

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
	engine: SingleThreadLazyResolve<StandardEngine<T>>,
	ioc: IocContainer,
	// Transaction-scoped change buffer
	changes: Rc<RefCell<Vec<FlowChange>>>,
}

impl<T: Transaction> TransactionalFlowInterceptor<T> {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			engine: SingleThreadLazyResolve::new(),
			ioc,
			changes: Rc::new(RefCell::new(Vec::new())),
		}
	}
}

impl<T: Transaction> Clone for TransactionalFlowInterceptor<T> {
	fn clone(&self) -> Self {
		Self {
			engine: self.engine.clone(),
			ioc: self.ioc.clone(),
			changes: Rc::clone(&self.changes),
		}
	}
}

impl<T: Transaction> TablePostInsertInterceptor<T>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(&self, ctx: &mut TablePostInsertContext<T>) -> Result<()> {
		self.changes.borrow_mut().push(FlowChange::Insert {
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
		self.changes.borrow_mut().push(FlowChange::Update {
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
		self.changes.borrow_mut().push(FlowChange::Delete {
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
		let engine = self.engine.get_or_resolve(&self.ioc)?;

		// Process all collected changes
		let mut changes = self.changes.borrow_mut();
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

impl<T: Transaction> RegisterInterceptor<T>
	for TransactionalFlowInterceptor<T>
{
	fn register(self: Rc<Self>, interceptors: &mut Interceptors<T>) {
		interceptors.table_post_insert.add(self.clone());
		interceptors.table_post_update.add(self.clone());
		interceptors.table_post_delete.add(self.clone());
		interceptors.pre_commit.add(self);
	}
}
