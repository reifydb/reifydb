// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use reifydb_type::RowNumber;

use crate::{
	CommitVersion,
	interceptor::{
		NamespaceDefPostCreateContext, NamespaceDefPostUpdateContext, NamespaceDefPreDeleteContext,
		NamespaceDefPreUpdateContext, PostCommitContext, PreCommitContext, RingBufferDefPostCreateContext,
		RingBufferDefPostUpdateContext, RingBufferDefPreDeleteContext, RingBufferDefPreUpdateContext,
		RingBufferPostDeleteContext, RingBufferPostInsertContext, RingBufferPostUpdateContext,
		RingBufferPreDeleteContext, RingBufferPreInsertContext, RingBufferPreUpdateContext,
		TableDefPostCreateContext, TableDefPostUpdateContext, TableDefPreDeleteContext,
		TableDefPreUpdateContext, TablePostDeleteContext, TablePostInsertContext, TablePostUpdateContext,
		TablePreDeleteContext, TablePreInsertContext, TablePreUpdateContext, ViewDefPostCreateContext,
		ViewDefPostUpdateContext, ViewDefPreDeleteContext, ViewDefPreUpdateContext,
	},
	interface::{
		CommandTransaction, NamespaceDef, RingBufferDef, RowChange, TableDef, TransactionId,
		TransactionalDefChanges, ViewDef,
		interceptor::{
			NamespaceDefInterceptor, RingBufferDefInterceptor, RingBufferInterceptor, TableDefInterceptor,
			TableInterceptor, TransactionInterceptor, ViewDefInterceptor, WithInterceptors,
		},
	},
	value::encoded::EncodedValues,
};

#[async_trait]
impl<CT: CommandTransaction + WithInterceptors<CT> + Send> TableInterceptor<CT> for CT {
	async fn pre_insert(&mut self, table: &TableDef, rn: RowNumber, row: &EncodedValues) -> crate::Result<()> {
		if self.table_pre_insert_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_pre_insert_interceptors().interceptors.clone();
		let mut ctx = TablePreInsertContext::new(self, table, rn, row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_insert(&mut self, table: &TableDef, id: RowNumber, row: &EncodedValues) -> crate::Result<()> {
		if self.table_post_insert_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_post_insert_interceptors().interceptors.clone();
		let mut ctx = TablePostInsertContext::new(self, table, id, row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_update(&mut self, table: &TableDef, id: RowNumber, row: &EncodedValues) -> crate::Result<()> {
		if self.table_pre_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_pre_update_interceptors().interceptors.clone();
		let mut ctx = TablePreUpdateContext::new(self, table, id, row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_update(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedValues,
		old_row: &EncodedValues,
	) -> crate::Result<()> {
		if self.table_post_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_post_update_interceptors().interceptors.clone();
		let mut ctx = TablePostUpdateContext::new(self, table, id, row, old_row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_delete(&mut self, table: &TableDef, id: RowNumber) -> crate::Result<()> {
		if self.table_pre_delete_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_pre_delete_interceptors().interceptors.clone();
		let mut ctx = TablePreDeleteContext::new(self, table, id);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_delete(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		deleted_row: &EncodedValues,
	) -> crate::Result<()> {
		if self.table_post_delete_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_post_delete_interceptors().interceptors.clone();
		let mut ctx = TablePostDeleteContext::new(self, table, id, deleted_row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + WithInterceptors<CT> + Send> RingBufferInterceptor<CT> for CT {
	async fn pre_insert(&mut self, ringbuffer: &RingBufferDef, row: &EncodedValues) -> crate::Result<()> {
		if self.ringbuffer_pre_insert_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_pre_insert_interceptors().interceptors.clone();
		let mut ctx = RingBufferPreInsertContext::new(self, ringbuffer, row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_insert(
		&mut self,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		row: &EncodedValues,
	) -> crate::Result<()> {
		if self.ringbuffer_post_insert_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_post_insert_interceptors().interceptors.clone();
		let mut ctx = RingBufferPostInsertContext::new(self, ringbuffer, id, row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_update(
		&mut self,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		row: &EncodedValues,
	) -> crate::Result<()> {
		if self.ringbuffer_pre_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_pre_update_interceptors().interceptors.clone();
		let mut ctx = RingBufferPreUpdateContext::new(self, ringbuffer, id, row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_update(
		&mut self,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		row: &EncodedValues,
		old_row: &EncodedValues,
	) -> crate::Result<()> {
		if self.ringbuffer_post_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_post_update_interceptors().interceptors.clone();
		let mut ctx = RingBufferPostUpdateContext::new(self, ringbuffer, id, row, old_row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_delete(&mut self, ringbuffer: &RingBufferDef, id: RowNumber) -> crate::Result<()> {
		if self.ringbuffer_pre_delete_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_pre_delete_interceptors().interceptors.clone();
		let mut ctx = RingBufferPreDeleteContext::new(self, ringbuffer, id);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_delete(
		&mut self,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		deleted_row: &EncodedValues,
	) -> crate::Result<()> {
		if self.ringbuffer_post_delete_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_post_delete_interceptors().interceptors.clone();
		let mut ctx = RingBufferPostDeleteContext::new(self, ringbuffer, id, deleted_row);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + WithInterceptors<CT> + Send> NamespaceDefInterceptor<CT> for CT {
	async fn post_create(&mut self, post: &NamespaceDef) -> crate::Result<()> {
		if self.namespace_def_post_create_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.namespace_def_post_create_interceptors().interceptors.clone();
		let mut ctx = NamespaceDefPostCreateContext::new(self, post);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_update(&mut self, pre: &NamespaceDef) -> crate::Result<()> {
		if self.namespace_def_pre_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.namespace_def_pre_update_interceptors().interceptors.clone();
		let mut ctx = NamespaceDefPreUpdateContext::new(self, pre);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_update(&mut self, pre: &NamespaceDef, post: &NamespaceDef) -> crate::Result<()> {
		if self.namespace_def_post_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.namespace_def_post_update_interceptors().interceptors.clone();
		let mut ctx = NamespaceDefPostUpdateContext::new(self, pre, post);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_delete(&mut self, pre: &NamespaceDef) -> crate::Result<()> {
		if self.namespace_def_pre_delete_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.namespace_def_pre_delete_interceptors().interceptors.clone();
		let mut ctx = NamespaceDefPreDeleteContext::new(self, pre);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + WithInterceptors<CT> + Send> TableDefInterceptor<CT> for CT {
	async fn post_create(&mut self, post: &TableDef) -> crate::Result<()> {
		if self.table_def_post_create_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_def_post_create_interceptors().interceptors.clone();
		let mut ctx = TableDefPostCreateContext::new(self, post);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_update(&mut self, pre: &TableDef) -> crate::Result<()> {
		if self.table_def_pre_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_def_pre_update_interceptors().interceptors.clone();
		let mut ctx = TableDefPreUpdateContext::new(self, pre);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_update(&mut self, pre: &TableDef, post: &TableDef) -> crate::Result<()> {
		if self.table_def_post_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_def_post_update_interceptors().interceptors.clone();
		let mut ctx = TableDefPostUpdateContext::new(self, pre, post);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_delete(&mut self, pre: &TableDef) -> crate::Result<()> {
		if self.table_def_pre_delete_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.table_def_pre_delete_interceptors().interceptors.clone();
		let mut ctx = TableDefPreDeleteContext::new(self, pre);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + WithInterceptors<CT> + Send> ViewDefInterceptor<CT> for CT {
	async fn post_create(&mut self, post: &ViewDef) -> crate::Result<()> {
		if self.view_def_post_create_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.view_def_post_create_interceptors().interceptors.clone();
		let mut ctx = ViewDefPostCreateContext::new(self, post);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_update(&mut self, pre: &ViewDef) -> crate::Result<()> {
		if self.view_def_pre_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.view_def_pre_update_interceptors().interceptors.clone();
		let mut ctx = ViewDefPreUpdateContext::new(self, pre);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_update(&mut self, pre: &ViewDef, post: &ViewDef) -> crate::Result<()> {
		if self.view_def_post_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.view_def_post_update_interceptors().interceptors.clone();
		let mut ctx = ViewDefPostUpdateContext::new(self, pre, post);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_delete(&mut self, pre: &ViewDef) -> crate::Result<()> {
		if self.view_def_pre_delete_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.view_def_pre_delete_interceptors().interceptors.clone();
		let mut ctx = ViewDefPreDeleteContext::new(self, pre);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + WithInterceptors<CT> + Send> RingBufferDefInterceptor<CT> for CT {
	async fn post_create(&mut self, post: &RingBufferDef) -> crate::Result<()> {
		if self.ringbuffer_def_post_create_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_def_post_create_interceptors().interceptors.clone();
		let mut ctx = RingBufferDefPostCreateContext::new(self, post);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_update(&mut self, pre: &RingBufferDef) -> crate::Result<()> {
		if self.ringbuffer_def_pre_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_def_pre_update_interceptors().interceptors.clone();
		let mut ctx = RingBufferDefPreUpdateContext::new(self, pre);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_update(&mut self, pre: &RingBufferDef, post: &RingBufferDef) -> crate::Result<()> {
		if self.ringbuffer_def_post_update_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_def_post_update_interceptors().interceptors.clone();
		let mut ctx = RingBufferDefPostUpdateContext::new(self, pre, post);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn pre_delete(&mut self, pre: &RingBufferDef) -> crate::Result<()> {
		if self.ringbuffer_def_pre_delete_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.ringbuffer_def_pre_delete_interceptors().interceptors.clone();
		let mut ctx = RingBufferDefPreDeleteContext::new(self, pre);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

#[async_trait]
impl<CT: CommandTransaction + WithInterceptors<CT> + Send> TransactionInterceptor<CT> for CT {
	async fn pre_commit(&mut self) -> crate::Result<()> {
		if self.pre_commit_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.pre_commit_interceptors().interceptors.clone();
		let mut ctx = PreCommitContext::new(self);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}

	async fn post_commit(
		&mut self,
		id: TransactionId,
		version: CommitVersion,
		changes: TransactionalDefChanges,
		row_changes: Vec<RowChange>,
	) -> crate::Result<()> {
		if self.post_commit_interceptors().is_empty() {
			return Ok(());
		}
		let interceptors = self.post_commit_interceptors().interceptors.clone();
		let mut ctx = PostCommitContext::new(id, version, changes, row_changes);
		for interceptor in interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}
