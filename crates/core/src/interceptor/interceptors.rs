// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::{
	interceptor::{
		Chain, InterceptorChain, NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateInterceptor,
		NamespaceDefPreDeleteInterceptor, NamespaceDefPreUpdateInterceptor, PostCommitInterceptor,
		PreCommitInterceptor, RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateInterceptor,
		RingBufferDefPreDeleteInterceptor, RingBufferDefPreUpdateInterceptor, RingBufferPostDeleteInterceptor,
		RingBufferPostInsertInterceptor, RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor,
		RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor, TableDefPostCreateInterceptor,
		TableDefPostUpdateInterceptor, TableDefPreDeleteInterceptor, TableDefPreUpdateInterceptor,
		TablePostDeleteInterceptor, TablePostInsertInterceptor, TablePostUpdateInterceptor,
		TablePreDeleteInterceptor, TablePreInsertInterceptor, TablePreUpdateInterceptor,
		ViewDefPostCreateInterceptor, ViewDefPostUpdateInterceptor, ViewDefPreDeleteInterceptor,
		ViewDefPreUpdateInterceptor,
	},
	interface::CommandTransaction,
};

/// Container for all interceptor chains
pub struct Interceptors<CT: CommandTransaction> {
	// Table data interceptors
	pub table_pre_insert: Chain<CT, dyn TablePreInsertInterceptor<CT> + Send + Sync>,
	pub table_post_insert: Chain<CT, dyn TablePostInsertInterceptor<CT> + Send + Sync>,
	pub table_pre_update: Chain<CT, dyn TablePreUpdateInterceptor<CT> + Send + Sync>,
	pub table_post_update: Chain<CT, dyn TablePostUpdateInterceptor<CT> + Send + Sync>,
	pub table_pre_delete: Chain<CT, dyn TablePreDeleteInterceptor<CT> + Send + Sync>,
	pub table_post_delete: Chain<CT, dyn TablePostDeleteInterceptor<CT> + Send + Sync>,
	// Ring buffer data interceptors
	pub ringbuffer_pre_insert: Chain<CT, dyn RingBufferPreInsertInterceptor<CT> + Send + Sync>,
	pub ringbuffer_post_insert: Chain<CT, dyn RingBufferPostInsertInterceptor<CT> + Send + Sync>,
	pub ringbuffer_pre_update: Chain<CT, dyn RingBufferPreUpdateInterceptor<CT> + Send + Sync>,
	pub ringbuffer_post_update: Chain<CT, dyn RingBufferPostUpdateInterceptor<CT> + Send + Sync>,
	pub ringbuffer_pre_delete: Chain<CT, dyn RingBufferPreDeleteInterceptor<CT> + Send + Sync>,
	pub ringbuffer_post_delete: Chain<CT, dyn RingBufferPostDeleteInterceptor<CT> + Send + Sync>,
	// Transaction interceptors
	pub pre_commit: Chain<CT, dyn PreCommitInterceptor<CT> + Send + Sync>,
	pub post_commit: Chain<CT, dyn PostCommitInterceptor<CT> + Send + Sync>,
	// Namespace definition interceptors
	pub namespace_def_post_create: Chain<CT, dyn NamespaceDefPostCreateInterceptor<CT> + Send + Sync>,
	pub namespace_def_pre_update: Chain<CT, dyn NamespaceDefPreUpdateInterceptor<CT> + Send + Sync>,
	pub namespace_def_post_update: Chain<CT, dyn NamespaceDefPostUpdateInterceptor<CT> + Send + Sync>,
	pub namespace_def_pre_delete: Chain<CT, dyn NamespaceDefPreDeleteInterceptor<CT> + Send + Sync>,
	// Table definition interceptors
	pub table_def_post_create: Chain<CT, dyn TableDefPostCreateInterceptor<CT> + Send + Sync>,
	pub table_def_pre_update: Chain<CT, dyn TableDefPreUpdateInterceptor<CT> + Send + Sync>,
	pub table_def_post_update: Chain<CT, dyn TableDefPostUpdateInterceptor<CT> + Send + Sync>,
	pub table_def_pre_delete: Chain<CT, dyn TableDefPreDeleteInterceptor<CT> + Send + Sync>,
	// View definition interceptors
	pub view_def_post_create: Chain<CT, dyn ViewDefPostCreateInterceptor<CT> + Send + Sync>,
	pub view_def_pre_update: Chain<CT, dyn ViewDefPreUpdateInterceptor<CT> + Send + Sync>,
	pub view_def_post_update: Chain<CT, dyn ViewDefPostUpdateInterceptor<CT> + Send + Sync>,
	pub view_def_pre_delete: Chain<CT, dyn ViewDefPreDeleteInterceptor<CT> + Send + Sync>,
	// Ring buffer definition interceptors
	pub ringbuffer_def_post_create: Chain<CT, dyn RingBufferDefPostCreateInterceptor<CT> + Send + Sync>,
	pub ringbuffer_def_pre_update: Chain<CT, dyn RingBufferDefPreUpdateInterceptor<CT> + Send + Sync>,
	pub ringbuffer_def_post_update: Chain<CT, dyn RingBufferDefPostUpdateInterceptor<CT> + Send + Sync>,
	pub ringbuffer_def_pre_delete: Chain<CT, dyn RingBufferDefPreDeleteInterceptor<CT> + Send + Sync>,
}

impl<CT: CommandTransaction> Default for Interceptors<CT> {
	fn default() -> Self {
		Self::new()
	}
}

impl<CT: CommandTransaction> Interceptors<CT> {
	pub fn new() -> Self {
		Self {
			table_pre_insert: InterceptorChain::new(),
			table_post_insert: InterceptorChain::new(),
			table_pre_update: InterceptorChain::new(),
			table_post_update: InterceptorChain::new(),
			table_pre_delete: InterceptorChain::new(),
			table_post_delete: InterceptorChain::new(),
			ringbuffer_pre_insert: InterceptorChain::new(),
			ringbuffer_post_insert: InterceptorChain::new(),
			ringbuffer_pre_update: InterceptorChain::new(),
			ringbuffer_post_update: InterceptorChain::new(),
			ringbuffer_pre_delete: InterceptorChain::new(),
			ringbuffer_post_delete: InterceptorChain::new(),
			pre_commit: InterceptorChain::new(),
			post_commit: InterceptorChain::new(),
			namespace_def_post_create: InterceptorChain::new(),
			namespace_def_pre_update: InterceptorChain::new(),
			namespace_def_post_update: InterceptorChain::new(),
			namespace_def_pre_delete: InterceptorChain::new(),
			table_def_post_create: InterceptorChain::new(),
			table_def_pre_update: InterceptorChain::new(),
			table_def_post_update: InterceptorChain::new(),
			table_def_pre_delete: InterceptorChain::new(),
			view_def_post_create: InterceptorChain::new(),
			view_def_pre_update: InterceptorChain::new(),
			view_def_post_update: InterceptorChain::new(),
			view_def_pre_delete: InterceptorChain::new(),
			ringbuffer_def_post_create: InterceptorChain::new(),
			ringbuffer_def_pre_update: InterceptorChain::new(),
			ringbuffer_def_post_update: InterceptorChain::new(),
			ringbuffer_def_pre_delete: InterceptorChain::new(),
		}
	}
}

impl<CT: CommandTransaction> Clone for Interceptors<CT> {
	fn clone(&self) -> Self {
		Self {
			table_pre_insert: self.table_pre_insert.clone(),
			table_post_insert: self.table_post_insert.clone(),
			table_pre_update: self.table_pre_update.clone(),
			table_post_update: self.table_post_update.clone(),
			table_pre_delete: self.table_pre_delete.clone(),
			table_post_delete: self.table_post_delete.clone(),
			ringbuffer_pre_insert: self.ringbuffer_pre_insert.clone(),
			ringbuffer_post_insert: self.ringbuffer_post_insert.clone(),
			ringbuffer_pre_update: self.ringbuffer_pre_update.clone(),
			ringbuffer_post_update: self.ringbuffer_post_update.clone(),
			ringbuffer_pre_delete: self.ringbuffer_pre_delete.clone(),
			ringbuffer_post_delete: self.ringbuffer_post_delete.clone(),
			pre_commit: self.pre_commit.clone(),
			post_commit: self.post_commit.clone(),
			namespace_def_post_create: self.namespace_def_post_create.clone(),
			namespace_def_pre_update: self.namespace_def_pre_update.clone(),
			namespace_def_post_update: self.namespace_def_post_update.clone(),
			namespace_def_pre_delete: self.namespace_def_pre_delete.clone(),
			table_def_post_create: self.table_def_post_create.clone(),
			table_def_pre_update: self.table_def_pre_update.clone(),
			table_def_post_update: self.table_def_post_update.clone(),
			table_def_pre_delete: self.table_def_pre_delete.clone(),
			view_def_post_create: self.view_def_post_create.clone(),
			view_def_pre_update: self.view_def_pre_update.clone(),
			view_def_post_update: self.view_def_post_update.clone(),
			view_def_pre_delete: self.view_def_pre_delete.clone(),
			ringbuffer_def_post_create: self.ringbuffer_def_post_create.clone(),
			ringbuffer_def_pre_update: self.ringbuffer_def_pre_update.clone(),
			ringbuffer_def_post_update: self.ringbuffer_def_post_update.clone(),
			ringbuffer_def_pre_delete: self.ringbuffer_def_pre_delete.clone(),
		}
	}
}

impl<CT: CommandTransaction> Interceptors<CT> {
	/// Register any interceptor - it will be added to all appropriate
	/// chains based on which traits it implements
	pub fn register<I>(&mut self, interceptor: I)
	where
		I: super::RegisterInterceptor<CT> + Send + Sync + 'static,
	{
		Arc::new(interceptor).register(self);
	}
}
