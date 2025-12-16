// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, rc::Rc};

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
		ViewDefPreUpdateInterceptor, ViewPostDeleteInterceptor, ViewPostInsertInterceptor,
		ViewPostUpdateInterceptor, ViewPreDeleteInterceptor, ViewPreInsertInterceptor,
		ViewPreUpdateInterceptor,
	},
	interface::CommandTransaction,
};

/// Container for all interceptor chains
pub struct Interceptors<CT: CommandTransaction> {
	// Table data interceptors
	pub table_pre_insert: Chain<CT, dyn TablePreInsertInterceptor<CT>>,
	pub table_post_insert: Chain<CT, dyn TablePostInsertInterceptor<CT>>,
	pub table_pre_update: Chain<CT, dyn TablePreUpdateInterceptor<CT>>,
	pub table_post_update: Chain<CT, dyn TablePostUpdateInterceptor<CT>>,
	pub table_pre_delete: Chain<CT, dyn TablePreDeleteInterceptor<CT>>,
	pub table_post_delete: Chain<CT, dyn TablePostDeleteInterceptor<CT>>,
	// Ring buffer data interceptors
	pub ringbuffer_pre_insert: Chain<CT, dyn RingBufferPreInsertInterceptor<CT>>,
	pub ringbuffer_post_insert: Chain<CT, dyn RingBufferPostInsertInterceptor<CT>>,
	pub ringbuffer_pre_update: Chain<CT, dyn RingBufferPreUpdateInterceptor<CT>>,
	pub ringbuffer_post_update: Chain<CT, dyn RingBufferPostUpdateInterceptor<CT>>,
	pub ringbuffer_pre_delete: Chain<CT, dyn RingBufferPreDeleteInterceptor<CT>>,
	pub ringbuffer_post_delete: Chain<CT, dyn RingBufferPostDeleteInterceptor<CT>>,
	// Transaction interceptors
	pub pre_commit: Chain<CT, dyn PreCommitInterceptor<CT>>,
	pub post_commit: Chain<CT, dyn PostCommitInterceptor<CT>>,
	// Namespace definition interceptors
	pub namespace_def_post_create: Chain<CT, dyn NamespaceDefPostCreateInterceptor<CT>>,
	pub namespace_def_pre_update: Chain<CT, dyn NamespaceDefPreUpdateInterceptor<CT>>,
	pub namespace_def_post_update: Chain<CT, dyn NamespaceDefPostUpdateInterceptor<CT>>,
	pub namespace_def_pre_delete: Chain<CT, dyn NamespaceDefPreDeleteInterceptor<CT>>,
	// Table definition interceptors
	pub table_def_post_create: Chain<CT, dyn TableDefPostCreateInterceptor<CT>>,
	pub table_def_pre_update: Chain<CT, dyn TableDefPreUpdateInterceptor<CT>>,
	pub table_def_post_update: Chain<CT, dyn TableDefPostUpdateInterceptor<CT>>,
	pub table_def_pre_delete: Chain<CT, dyn TableDefPreDeleteInterceptor<CT>>,
	// View definition interceptors
	pub view_def_post_create: Chain<CT, dyn ViewDefPostCreateInterceptor<CT>>,
	pub view_def_pre_update: Chain<CT, dyn ViewDefPreUpdateInterceptor<CT>>,
	pub view_def_post_update: Chain<CT, dyn ViewDefPostUpdateInterceptor<CT>>,
	pub view_def_pre_delete: Chain<CT, dyn ViewDefPreDeleteInterceptor<CT>>,
	// Ring buffer definition interceptors
	pub ringbuffer_def_post_create: Chain<CT, dyn RingBufferDefPostCreateInterceptor<CT>>,
	pub ringbuffer_def_pre_update: Chain<CT, dyn RingBufferDefPreUpdateInterceptor<CT>>,
	pub ringbuffer_def_post_update: Chain<CT, dyn RingBufferDefPostUpdateInterceptor<CT>>,
	pub ringbuffer_def_pre_delete: Chain<CT, dyn RingBufferDefPreDeleteInterceptor<CT>>,
	// View data interceptors
	pub view_pre_insert: Chain<CT, dyn ViewPreInsertInterceptor<CT>>,
	pub view_post_insert: Chain<CT, dyn ViewPostInsertInterceptor<CT>>,
	pub view_pre_update: Chain<CT, dyn ViewPreUpdateInterceptor<CT>>,
	pub view_post_update: Chain<CT, dyn ViewPostUpdateInterceptor<CT>>,
	pub view_pre_delete: Chain<CT, dyn ViewPreDeleteInterceptor<CT>>,
	pub view_post_delete: Chain<CT, dyn ViewPostDeleteInterceptor<CT>>,
	// Marker to prevent Send and Sync
	_not_send_sync: PhantomData<*const ()>,
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
			view_pre_insert: InterceptorChain::new(),
			view_post_insert: InterceptorChain::new(),
			view_pre_update: InterceptorChain::new(),
			view_post_update: InterceptorChain::new(),
			view_pre_delete: InterceptorChain::new(),
			view_post_delete: InterceptorChain::new(),
			_not_send_sync: PhantomData,
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
			view_pre_insert: self.view_pre_insert.clone(),
			view_post_insert: self.view_post_insert.clone(),
			view_pre_update: self.view_pre_update.clone(),
			view_post_update: self.view_post_update.clone(),
			view_pre_delete: self.view_pre_delete.clone(),
			view_post_delete: self.view_post_delete.clone(),
			_not_send_sync: PhantomData,
		}
	}
}

impl<CT: CommandTransaction> Interceptors<CT> {
	/// Register any interceptor - it will be added to all appropriate
	/// chains based on which traits it implements
	pub fn register<I>(&mut self, interceptor: I)
	where
		I: super::RegisterInterceptor<CT> + 'static,
	{
		Rc::new(interceptor).register(self);
	}
}
