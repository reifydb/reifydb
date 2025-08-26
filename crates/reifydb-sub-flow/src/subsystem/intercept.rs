// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{cell::RefCell, rc::Rc};

use reifydb_core::{
	Result, RowNumber,
	interceptor::{
		Interceptors, PreCommitContext, PreCommitInterceptor,
		RegisterInterceptor, TablePostDeleteContext,
		TablePostDeleteInterceptor, TablePostInsertContext,
		TablePostInsertInterceptor, TablePostUpdateContext,
		TablePostUpdateInterceptor,
	},
	interface::{CommandTransaction, TableId, Transaction},
	ioc::{IocContainer, LazyResolveRc},
};
use reifydb_engine::StandardEngine;

/// Event type for flow processing
#[derive(Debug, Clone)]
pub(crate) enum Change {
	Insert {
		table_id: TableId,
		row_number: RowNumber,
		row: Vec<u8>,
	},
	Update {
		table_id: TableId,
		row_number: RowNumber,
		before: Vec<u8>,
		after: Vec<u8>,
	},
	Delete {
		table_id: TableId,
		row_number: RowNumber,
		row: Vec<u8>,
	},
}

pub struct TransactionalFlowInterceptor<T: Transaction> {
	engine: LazyResolveRc<StandardEngine<T>>,
	ioc: IocContainer,
	// Transaction-scoped change buffer
	changes: Rc<RefCell<Vec<Change>>>,
}

impl<T: Transaction> TransactionalFlowInterceptor<T> {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			engine: LazyResolveRc::new(),
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

impl<T: Transaction, CT: CommandTransaction> TablePostInsertInterceptor<CT>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(
		&self,
		ctx: &mut TablePostInsertContext<CT>,
	) -> Result<()> {
		self.changes.borrow_mut().push(Change::Insert {
			table_id: ctx.table.id,
			row_number: ctx.id,
			row: ctx.row.to_vec(),
		});

		Ok(())
	}
}

impl<T: Transaction, CT: CommandTransaction> TablePostUpdateInterceptor<CT>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(
		&self,
		ctx: &mut TablePostUpdateContext<CT>,
	) -> Result<()> {
		self.changes.borrow_mut().push(Change::Update {
			table_id: ctx.table.id,
			row_number: ctx.id,
			before: ctx.old_row.to_vec(),
			after: ctx.row.to_vec(),
		});
		Ok(())
	}
}

impl<T: Transaction, CT: CommandTransaction> TablePostDeleteInterceptor<CT>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(
		&self,
		ctx: &mut TablePostDeleteContext<CT>,
	) -> Result<()> {
		self.changes.borrow_mut().push(Change::Delete {
			table_id: ctx.table.id,
			row_number: ctx.id,
			row: ctx.deleted_row.to_vec(),
		});
		Ok(())
	}
}

impl<T: Transaction, CT: CommandTransaction> PreCommitInterceptor<CT>
	for TransactionalFlowInterceptor<T>
{
	fn intercept(&self, _ctx: &mut PreCommitContext<CT>) -> Result<()> {
		let _engine = self.engine.get_or_resolve(&self.ioc)?;

		// Process all collected changes through flow engine
		let changes = self.changes.borrow_mut();
		if !changes.is_empty() {
			// TODO: Convert FlowChange to flow engine Change format
			// and process through flow engine
			// for change in changes.drain(..) {
			// 	log_debug!("Intercepted change: {:?}", change);
			// 	// The flow engine will be accessed via the
			// engine/subsystem 	// This interceptor collects
			// changes for the flow engine }
		}

		Ok(())
	}
}

impl<T: Transaction, CT: CommandTransaction> RegisterInterceptor<CT>
	for TransactionalFlowInterceptor<T>
{
	fn register(self: Rc<Self>, interceptors: &mut Interceptors<CT>) {
		interceptors.table_post_insert.add(self.clone());
		interceptors.table_post_update.add(self.clone());
		interceptors.table_post_delete.add(self.clone());
		interceptors.pre_commit.add(self);
	}
}
