// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{cell::RefCell, rc::Rc};

use reifydb_core::{
	Result,
	interceptor::{
		Interceptors, PreCommitContext, PreCommitInterceptor, RegisterInterceptor, RingBufferPostDeleteContext,
		RingBufferPostDeleteInterceptor, RingBufferPostInsertContext, RingBufferPostInsertInterceptor,
		RingBufferPostUpdateContext, RingBufferPostUpdateInterceptor, TablePostDeleteContext,
		TablePostDeleteInterceptor, TablePostInsertContext, TablePostInsertInterceptor, TablePostUpdateContext,
		TablePostUpdateInterceptor,
	},
	interface::{CommandTransaction, SourceId},
	ioc::{IocContainer, LazyResolveRc},
};
use reifydb_engine::StandardEngine;
use reifydb_type::RowNumber;

/// Event type for flow processing
#[derive(Debug, Clone)]
pub(crate) enum Change {
	Insert {
		_source_id: SourceId,
		row_number: RowNumber,
		post: Vec<u8>,
	},
	Update {
		_source_id: SourceId,
		row_number: RowNumber,
		pre: Vec<u8>,
		post: Vec<u8>,
	},
	Delete {
		_source_id: SourceId,
		row_number: RowNumber,
		pre: Vec<u8>,
	},
}

pub struct TransactionalFlowInterceptor {
	engine: LazyResolveRc<StandardEngine>,
	ioc: IocContainer,
	// Transaction-scoped change buffer
	changes: Rc<RefCell<Vec<Change>>>,
}

impl TransactionalFlowInterceptor {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			engine: LazyResolveRc::new(),
			ioc,
			changes: Rc::new(RefCell::new(Vec::new())),
		}
	}
}

impl Clone for TransactionalFlowInterceptor {
	fn clone(&self) -> Self {
		Self {
			engine: self.engine.clone(),
			ioc: self.ioc.clone(),
			changes: Rc::clone(&self.changes),
		}
	}
}

impl<CT: CommandTransaction> TablePostInsertInterceptor<CT> for TransactionalFlowInterceptor {
	fn intercept(&self, ctx: &mut TablePostInsertContext<CT>) -> Result<()> {
		self.changes.borrow_mut().push(Change::Insert {
			_source_id: SourceId::from(ctx.table.id),
			row_number: ctx.id,
			post: ctx.row.to_vec(),
		});

		Ok(())
	}
}

impl<CT: CommandTransaction> TablePostUpdateInterceptor<CT> for TransactionalFlowInterceptor {
	fn intercept(&self, ctx: &mut TablePostUpdateContext<CT>) -> Result<()> {
		self.changes.borrow_mut().push(Change::Update {
			_source_id: SourceId::from(ctx.table.id),
			row_number: ctx.id,
			pre: ctx.old_row.to_vec(),
			post: ctx.row.to_vec(),
		});
		Ok(())
	}
}

impl<CT: CommandTransaction> TablePostDeleteInterceptor<CT> for TransactionalFlowInterceptor {
	fn intercept(&self, ctx: &mut TablePostDeleteContext<CT>) -> Result<()> {
		self.changes.borrow_mut().push(Change::Delete {
			_source_id: SourceId::from(ctx.table.id),
			row_number: ctx.id,
			pre: ctx.deleted_row.to_vec(),
		});
		Ok(())
	}
}

impl<CT: CommandTransaction> RingBufferPostInsertInterceptor<CT> for TransactionalFlowInterceptor {
	fn intercept(&self, ctx: &mut RingBufferPostInsertContext<CT>) -> Result<()> {
		self.changes.borrow_mut().push(Change::Insert {
			_source_id: SourceId::from(ctx.ringbuffer.id),
			row_number: ctx.id,
			post: ctx.row.to_vec(),
		});

		Ok(())
	}
}

impl<CT: CommandTransaction> RingBufferPostUpdateInterceptor<CT> for TransactionalFlowInterceptor {
	fn intercept(&self, ctx: &mut RingBufferPostUpdateContext<CT>) -> Result<()> {
		self.changes.borrow_mut().push(Change::Update {
			_source_id: SourceId::from(ctx.ringbuffer.id),
			row_number: ctx.id,
			pre: ctx.old_row.to_vec(),
			post: ctx.row.to_vec(),
		});
		Ok(())
	}
}

impl<CT: CommandTransaction> RingBufferPostDeleteInterceptor<CT> for TransactionalFlowInterceptor {
	fn intercept(&self, ctx: &mut RingBufferPostDeleteContext<CT>) -> Result<()> {
		self.changes.borrow_mut().push(Change::Delete {
			_source_id: SourceId::from(ctx.ringbuffer.id),
			row_number: ctx.id,
			pre: ctx.deleted_row.to_vec(),
		});
		Ok(())
	}
}

impl<CT: CommandTransaction> PreCommitInterceptor<CT> for TransactionalFlowInterceptor {
	fn intercept(&self, _ctx: &mut PreCommitContext<CT>) -> Result<()> {
		let _engine = self.engine.get_or_resolve(&self.ioc)?;

		// Process all collected changes through flow engine
		let changes = self.changes.borrow_mut();
		if !changes.is_empty() {
			// TODO: Convert FlowChange to flow engine Change format
			// and process through flow engine
			// for change in changes.drain(..) {
			// 	debug!("Intercepted change: {:?}", change);
			// 	// The flow engine will be accessed via the
			// engine/subsystem 	// This interceptor collects
			// changes for the flow engine }
		}

		Ok(())
	}
}

impl<CT: CommandTransaction> RegisterInterceptor<CT> for TransactionalFlowInterceptor {
	fn register(self: Rc<Self>, interceptors: &mut Interceptors<CT>) {
		interceptors.table_post_insert.add(self.clone());
		interceptors.table_post_update.add(self.clone());
		interceptors.table_post_delete.add(self.clone());
		interceptors.ringbuffer_post_insert.add(self.clone());
		interceptors.ringbuffer_post_update.add(self.clone());
		interceptors.ringbuffer_post_delete.add(self.clone());
		interceptors.pre_commit.add(self);
	}
}
