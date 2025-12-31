// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	Result,
	interceptor::{
		Interceptors, PreCommitContext, PreCommitInterceptor, RegisterInterceptor, RingBufferPostDeleteContext,
		RingBufferPostDeleteInterceptor, RingBufferPostInsertContext, RingBufferPostInsertInterceptor,
		RingBufferPostUpdateContext, RingBufferPostUpdateInterceptor, TablePostDeleteContext,
		TablePostDeleteInterceptor, TablePostInsertContext, TablePostInsertInterceptor, TablePostUpdateContext,
		TablePostUpdateInterceptor,
	},
	interface::CommandTransaction,
	ioc::{IocContainer, LazyResolveArc},
};
use reifydb_engine::StandardEngine;
use reifydb_type::RowNumber;
use tokio::sync::Mutex;

/// Event type for flow processing
#[derive(Debug, Clone)]
pub(crate) enum Change {
	Insert {
		row_number: RowNumber,
		post: Vec<u8>,
	},
	Update {
		row_number: RowNumber,
		pre: Vec<u8>,
		post: Vec<u8>,
	},
	Delete {
		row_number: RowNumber,
		pre: Vec<u8>,
	},
}

pub struct TransactionalFlowInterceptor {
	engine: LazyResolveArc<StandardEngine>,
	ioc: IocContainer,
	// Transaction-scoped change buffer
	changes: Arc<Mutex<Vec<Change>>>,
}

impl TransactionalFlowInterceptor {
	pub fn new(ioc: IocContainer) -> Self {
		Self {
			engine: LazyResolveArc::new(),
			ioc,
			changes: Arc::new(Mutex::new(Vec::new())),
		}
	}
}

impl Clone for TransactionalFlowInterceptor {
	fn clone(&self) -> Self {
		Self {
			engine: self.engine.clone(),
			ioc: self.ioc.clone(),
			changes: Arc::clone(&self.changes),
		}
	}
}

#[async_trait]
impl<CT: CommandTransaction + Send> TablePostInsertInterceptor<CT> for TransactionalFlowInterceptor {
	async fn intercept<'a>(&self, ctx: &mut TablePostInsertContext<'a, CT>) -> Result<()> {
		self.changes.lock().await.push(Change::Insert {
			row_number: ctx.id,
			post: ctx.row.to_vec(),
		});

		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + Send> TablePostUpdateInterceptor<CT> for TransactionalFlowInterceptor {
	async fn intercept<'a>(&self, ctx: &mut TablePostUpdateContext<'a, CT>) -> Result<()> {
		self.changes.lock().await.push(Change::Update {
			row_number: ctx.id,
			pre: ctx.old_row.to_vec(),
			post: ctx.row.to_vec(),
		});
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + Send> TablePostDeleteInterceptor<CT> for TransactionalFlowInterceptor {
	async fn intercept<'a>(&self, ctx: &mut TablePostDeleteContext<'a, CT>) -> Result<()> {
		self.changes.lock().await.push(Change::Delete {
			row_number: ctx.id,
			pre: ctx.deleted_row.to_vec(),
		});
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + Send> RingBufferPostInsertInterceptor<CT> for TransactionalFlowInterceptor {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostInsertContext<'a, CT>) -> Result<()> {
		self.changes.lock().await.push(Change::Insert {
			row_number: ctx.id,
			post: ctx.row.to_vec(),
		});

		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + Send> RingBufferPostUpdateInterceptor<CT> for TransactionalFlowInterceptor {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostUpdateContext<'a, CT>) -> Result<()> {
		self.changes.lock().await.push(Change::Update {
			row_number: ctx.id,
			pre: ctx.old_row.to_vec(),
			post: ctx.row.to_vec(),
		});
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + Send> RingBufferPostDeleteInterceptor<CT> for TransactionalFlowInterceptor {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostDeleteContext<'a, CT>) -> Result<()> {
		self.changes.lock().await.push(Change::Delete {
			row_number: ctx.id,
			pre: ctx.deleted_row.to_vec(),
		});
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + Send> PreCommitInterceptor<CT> for TransactionalFlowInterceptor {
	async fn intercept<'a>(&self, _ctx: &mut PreCommitContext<'a, CT>) -> Result<()> {
		let _engine = self.engine.get_or_resolve(&self.ioc)?;

		// Process all collected changes through flow engine
		let changes = self.changes.lock().await;
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

impl<CT: CommandTransaction + Send + 'static> RegisterInterceptor<CT> for TransactionalFlowInterceptor {
	fn register(self: Arc<Self>, interceptors: &mut Interceptors<CT>) {
		interceptors.table_post_insert.add(self.clone());
		interceptors.table_post_update.add(self.clone());
		interceptors.table_post_delete.add(self.clone());
		interceptors.ringbuffer_post_insert.add(self.clone());
		interceptors.ringbuffer_post_update.add(self.clone());
		interceptors.ringbuffer_post_delete.add(self.clone());
		interceptors.pre_commit.add(self);
	}
}
