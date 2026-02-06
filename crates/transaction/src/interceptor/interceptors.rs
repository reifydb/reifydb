// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{
	chain::InterceptorChain,
	namespace_def::{
		NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateInterceptor, NamespaceDefPreDeleteInterceptor,
		NamespaceDefPreUpdateInterceptor,
	},
	ringbuffer::{
		RingBufferPostDeleteInterceptor, RingBufferPostInsertInterceptor, RingBufferPostUpdateInterceptor,
		RingBufferPreDeleteInterceptor, RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor,
	},
	ringbuffer_def::{
		RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateInterceptor,
		RingBufferDefPreDeleteInterceptor, RingBufferDefPreUpdateInterceptor,
	},
	table::{
		TablePostDeleteInterceptor, TablePostInsertInterceptor, TablePostUpdateInterceptor,
		TablePreDeleteInterceptor, TablePreInsertInterceptor, TablePreUpdateInterceptor,
	},
	table_def::{
		TableDefPostCreateInterceptor, TableDefPostUpdateInterceptor, TableDefPreDeleteInterceptor,
		TableDefPreUpdateInterceptor,
	},
	transaction::{PostCommitInterceptor, PreCommitInterceptor},
	view::{
		ViewPostDeleteInterceptor, ViewPostInsertInterceptor, ViewPostUpdateInterceptor,
		ViewPreDeleteInterceptor, ViewPreInsertInterceptor, ViewPreUpdateInterceptor,
	},
	view_def::{
		ViewDefPostCreateInterceptor, ViewDefPostUpdateInterceptor, ViewDefPreDeleteInterceptor,
		ViewDefPreUpdateInterceptor,
	},
};

/// Type alias for interceptor chains
pub type Chain<I> = InterceptorChain<I>;

/// Container for all interceptor chains
pub struct Interceptors {
	// Table data interceptors
	pub table_pre_insert: Chain<dyn TablePreInsertInterceptor + Send + Sync>,
	pub table_post_insert: Chain<dyn TablePostInsertInterceptor + Send + Sync>,
	pub table_pre_update: Chain<dyn TablePreUpdateInterceptor + Send + Sync>,
	pub table_post_update: Chain<dyn TablePostUpdateInterceptor + Send + Sync>,
	pub table_pre_delete: Chain<dyn TablePreDeleteInterceptor + Send + Sync>,
	pub table_post_delete: Chain<dyn TablePostDeleteInterceptor + Send + Sync>,
	// Ring buffer data interceptors
	pub ringbuffer_pre_insert: Chain<dyn RingBufferPreInsertInterceptor + Send + Sync>,
	pub ringbuffer_post_insert: Chain<dyn RingBufferPostInsertInterceptor + Send + Sync>,
	pub ringbuffer_pre_update: Chain<dyn RingBufferPreUpdateInterceptor + Send + Sync>,
	pub ringbuffer_post_update: Chain<dyn RingBufferPostUpdateInterceptor + Send + Sync>,
	pub ringbuffer_pre_delete: Chain<dyn RingBufferPreDeleteInterceptor + Send + Sync>,
	pub ringbuffer_post_delete: Chain<dyn RingBufferPostDeleteInterceptor + Send + Sync>,
	// Transaction interceptors
	pub pre_commit: Chain<dyn PreCommitInterceptor + Send + Sync>,
	pub post_commit: Chain<dyn PostCommitInterceptor + Send + Sync>,
	// Namespace definition interceptors
	pub namespace_def_post_create: Chain<dyn NamespaceDefPostCreateInterceptor + Send + Sync>,
	pub namespace_def_pre_update: Chain<dyn NamespaceDefPreUpdateInterceptor + Send + Sync>,
	pub namespace_def_post_update: Chain<dyn NamespaceDefPostUpdateInterceptor + Send + Sync>,
	pub namespace_def_pre_delete: Chain<dyn NamespaceDefPreDeleteInterceptor + Send + Sync>,
	// Table definition interceptors
	pub table_def_post_create: Chain<dyn TableDefPostCreateInterceptor + Send + Sync>,
	pub table_def_pre_update: Chain<dyn TableDefPreUpdateInterceptor + Send + Sync>,
	pub table_def_post_update: Chain<dyn TableDefPostUpdateInterceptor + Send + Sync>,
	pub table_def_pre_delete: Chain<dyn TableDefPreDeleteInterceptor + Send + Sync>,
	// View data interceptors
	pub view_pre_insert: Chain<dyn ViewPreInsertInterceptor + Send + Sync>,
	pub view_post_insert: Chain<dyn ViewPostInsertInterceptor + Send + Sync>,
	pub view_pre_update: Chain<dyn ViewPreUpdateInterceptor + Send + Sync>,
	pub view_post_update: Chain<dyn ViewPostUpdateInterceptor + Send + Sync>,
	pub view_pre_delete: Chain<dyn ViewPreDeleteInterceptor + Send + Sync>,
	pub view_post_delete: Chain<dyn ViewPostDeleteInterceptor + Send + Sync>,
	// View definition interceptors
	pub view_def_post_create: Chain<dyn ViewDefPostCreateInterceptor + Send + Sync>,
	pub view_def_pre_update: Chain<dyn ViewDefPreUpdateInterceptor + Send + Sync>,
	pub view_def_post_update: Chain<dyn ViewDefPostUpdateInterceptor + Send + Sync>,
	pub view_def_pre_delete: Chain<dyn ViewDefPreDeleteInterceptor + Send + Sync>,
	// Ring buffer definition interceptors
	pub ringbuffer_def_post_create: Chain<dyn RingBufferDefPostCreateInterceptor + Send + Sync>,
	pub ringbuffer_def_pre_update: Chain<dyn RingBufferDefPreUpdateInterceptor + Send + Sync>,
	pub ringbuffer_def_post_update: Chain<dyn RingBufferDefPostUpdateInterceptor + Send + Sync>,
	pub ringbuffer_def_pre_delete: Chain<dyn RingBufferDefPreDeleteInterceptor + Send + Sync>,
}

impl Default for Interceptors {
	fn default() -> Self {
		Self::new()
	}
}

impl Interceptors {
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
			view_pre_insert: InterceptorChain::new(),
			view_post_insert: InterceptorChain::new(),
			view_pre_update: InterceptorChain::new(),
			view_post_update: InterceptorChain::new(),
			view_pre_delete: InterceptorChain::new(),
			view_post_delete: InterceptorChain::new(),
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

/// Trait for types that can register themselves with an interceptor container
pub trait RegisterInterceptor: Send + Sync {
	fn register(self, interceptors: &mut Interceptors);
}

impl Clone for Interceptors {
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
			view_pre_insert: self.view_pre_insert.clone(),
			view_post_insert: self.view_post_insert.clone(),
			view_pre_update: self.view_pre_update.clone(),
			view_post_update: self.view_post_update.clone(),
			view_pre_delete: self.view_pre_delete.clone(),
			view_post_delete: self.view_post_delete.clone(),
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
