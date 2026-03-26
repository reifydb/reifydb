// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{
	authentication_def::{AuthenticationDefPostCreateInterceptor, AuthenticationDefPreDeleteInterceptor},
	chain::InterceptorChain,
	dictionary::{
		DictionaryPostDeleteInterceptor, DictionaryPostInsertInterceptor, DictionaryPostUpdateInterceptor,
		DictionaryPreDeleteInterceptor, DictionaryPreInsertInterceptor, DictionaryPreUpdateInterceptor,
	},
	dictionary_def::{
		DictionaryDefPostCreateInterceptor, DictionaryDefPostUpdateInterceptor,
		DictionaryDefPreDeleteInterceptor, DictionaryDefPreUpdateInterceptor,
	},
	identity_def::{
		IdentityDefPostCreateInterceptor, IdentityDefPostUpdateInterceptor, IdentityDefPreDeleteInterceptor,
		IdentityDefPreUpdateInterceptor,
	},
	identity_role_def::{IdentityRoleDefPostCreateInterceptor, IdentityRoleDefPreDeleteInterceptor},
	namespace::{
		NamespacePostCreateInterceptor, NamespacePostUpdateInterceptor, NamespacePreDeleteInterceptor,
		NamespacePreUpdateInterceptor,
	},
	ringbuffer::{
		RingBufferPostDeleteInterceptor, RingBufferPostInsertInterceptor, RingBufferPostUpdateInterceptor,
		RingBufferPreDeleteInterceptor, RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor,
	},
	ringbuffer_def::{
		RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateInterceptor,
		RingBufferDefPreDeleteInterceptor, RingBufferDefPreUpdateInterceptor,
	},
	role_def::{
		RoleDefPostCreateInterceptor, RoleDefPostUpdateInterceptor, RoleDefPreDeleteInterceptor,
		RoleDefPreUpdateInterceptor,
	},
	series::{
		SeriesPostDeleteInterceptor, SeriesPostInsertInterceptor, SeriesPostUpdateInterceptor,
		SeriesPreDeleteInterceptor, SeriesPreInsertInterceptor, SeriesPreUpdateInterceptor,
	},
	series_def::{
		SeriesDefPostCreateInterceptor, SeriesDefPostUpdateInterceptor, SeriesDefPreDeleteInterceptor,
		SeriesDefPreUpdateInterceptor,
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
	pub namespace_post_create: Chain<dyn NamespacePostCreateInterceptor + Send + Sync>,
	pub namespace_pre_update: Chain<dyn NamespacePreUpdateInterceptor + Send + Sync>,
	pub namespace_post_update: Chain<dyn NamespacePostUpdateInterceptor + Send + Sync>,
	pub namespace_pre_delete: Chain<dyn NamespacePreDeleteInterceptor + Send + Sync>,
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
	// Dictionary data interceptors
	pub dictionary_pre_insert: Chain<dyn DictionaryPreInsertInterceptor + Send + Sync>,
	pub dictionary_post_insert: Chain<dyn DictionaryPostInsertInterceptor + Send + Sync>,
	pub dictionary_pre_update: Chain<dyn DictionaryPreUpdateInterceptor + Send + Sync>,
	pub dictionary_post_update: Chain<dyn DictionaryPostUpdateInterceptor + Send + Sync>,
	pub dictionary_pre_delete: Chain<dyn DictionaryPreDeleteInterceptor + Send + Sync>,
	pub dictionary_post_delete: Chain<dyn DictionaryPostDeleteInterceptor + Send + Sync>,
	// Dictionary definition interceptors
	pub dictionary_def_post_create: Chain<dyn DictionaryDefPostCreateInterceptor + Send + Sync>,
	pub dictionary_def_pre_update: Chain<dyn DictionaryDefPreUpdateInterceptor + Send + Sync>,
	pub dictionary_def_post_update: Chain<dyn DictionaryDefPostUpdateInterceptor + Send + Sync>,
	pub dictionary_def_pre_delete: Chain<dyn DictionaryDefPreDeleteInterceptor + Send + Sync>,
	// Series data interceptors
	pub series_pre_insert: Chain<dyn SeriesPreInsertInterceptor + Send + Sync>,
	pub series_post_insert: Chain<dyn SeriesPostInsertInterceptor + Send + Sync>,
	pub series_pre_update: Chain<dyn SeriesPreUpdateInterceptor + Send + Sync>,
	pub series_post_update: Chain<dyn SeriesPostUpdateInterceptor + Send + Sync>,
	pub series_pre_delete: Chain<dyn SeriesPreDeleteInterceptor + Send + Sync>,
	pub series_post_delete: Chain<dyn SeriesPostDeleteInterceptor + Send + Sync>,
	// Series definition interceptors
	pub series_def_post_create: Chain<dyn SeriesDefPostCreateInterceptor + Send + Sync>,
	pub series_def_pre_update: Chain<dyn SeriesDefPreUpdateInterceptor + Send + Sync>,
	pub series_def_post_update: Chain<dyn SeriesDefPostUpdateInterceptor + Send + Sync>,
	pub series_def_pre_delete: Chain<dyn SeriesDefPreDeleteInterceptor + Send + Sync>,
	// Identity definition interceptors
	pub identity_def_post_create: Chain<dyn IdentityDefPostCreateInterceptor + Send + Sync>,
	pub identity_def_pre_update: Chain<dyn IdentityDefPreUpdateInterceptor + Send + Sync>,
	pub identity_def_post_update: Chain<dyn IdentityDefPostUpdateInterceptor + Send + Sync>,
	pub identity_def_pre_delete: Chain<dyn IdentityDefPreDeleteInterceptor + Send + Sync>,
	// Role definition interceptors
	pub role_def_post_create: Chain<dyn RoleDefPostCreateInterceptor + Send + Sync>,
	pub role_def_pre_update: Chain<dyn RoleDefPreUpdateInterceptor + Send + Sync>,
	pub role_def_post_update: Chain<dyn RoleDefPostUpdateInterceptor + Send + Sync>,
	pub role_def_pre_delete: Chain<dyn RoleDefPreDeleteInterceptor + Send + Sync>,
	// Identity-role definition interceptors
	pub identity_role_def_post_create: Chain<dyn IdentityRoleDefPostCreateInterceptor + Send + Sync>,
	pub identity_role_def_pre_delete: Chain<dyn IdentityRoleDefPreDeleteInterceptor + Send + Sync>,
	// Authentication definition interceptors
	pub authentication_def_post_create: Chain<dyn AuthenticationDefPostCreateInterceptor + Send + Sync>,
	pub authentication_def_pre_delete: Chain<dyn AuthenticationDefPreDeleteInterceptor + Send + Sync>,
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
			namespace_post_create: InterceptorChain::new(),
			namespace_pre_update: InterceptorChain::new(),
			namespace_post_update: InterceptorChain::new(),
			namespace_pre_delete: InterceptorChain::new(),
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
			dictionary_pre_insert: InterceptorChain::new(),
			dictionary_post_insert: InterceptorChain::new(),
			dictionary_pre_update: InterceptorChain::new(),
			dictionary_post_update: InterceptorChain::new(),
			dictionary_pre_delete: InterceptorChain::new(),
			dictionary_post_delete: InterceptorChain::new(),
			dictionary_def_post_create: InterceptorChain::new(),
			dictionary_def_pre_update: InterceptorChain::new(),
			dictionary_def_post_update: InterceptorChain::new(),
			dictionary_def_pre_delete: InterceptorChain::new(),
			series_pre_insert: InterceptorChain::new(),
			series_post_insert: InterceptorChain::new(),
			series_pre_update: InterceptorChain::new(),
			series_post_update: InterceptorChain::new(),
			series_pre_delete: InterceptorChain::new(),
			series_post_delete: InterceptorChain::new(),
			series_def_post_create: InterceptorChain::new(),
			series_def_pre_update: InterceptorChain::new(),
			series_def_post_update: InterceptorChain::new(),
			series_def_pre_delete: InterceptorChain::new(),
			identity_def_post_create: InterceptorChain::new(),
			identity_def_pre_update: InterceptorChain::new(),
			identity_def_post_update: InterceptorChain::new(),
			identity_def_pre_delete: InterceptorChain::new(),
			role_def_post_create: InterceptorChain::new(),
			role_def_pre_update: InterceptorChain::new(),
			role_def_post_update: InterceptorChain::new(),
			role_def_pre_delete: InterceptorChain::new(),
			identity_role_def_post_create: InterceptorChain::new(),
			identity_role_def_pre_delete: InterceptorChain::new(),
			authentication_def_post_create: InterceptorChain::new(),
			authentication_def_pre_delete: InterceptorChain::new(),
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
			namespace_post_create: self.namespace_post_create.clone(),
			namespace_pre_update: self.namespace_pre_update.clone(),
			namespace_post_update: self.namespace_post_update.clone(),
			namespace_pre_delete: self.namespace_pre_delete.clone(),
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
			dictionary_pre_insert: self.dictionary_pre_insert.clone(),
			dictionary_post_insert: self.dictionary_post_insert.clone(),
			dictionary_pre_update: self.dictionary_pre_update.clone(),
			dictionary_post_update: self.dictionary_post_update.clone(),
			dictionary_pre_delete: self.dictionary_pre_delete.clone(),
			dictionary_post_delete: self.dictionary_post_delete.clone(),
			dictionary_def_post_create: self.dictionary_def_post_create.clone(),
			dictionary_def_pre_update: self.dictionary_def_pre_update.clone(),
			dictionary_def_post_update: self.dictionary_def_post_update.clone(),
			dictionary_def_pre_delete: self.dictionary_def_pre_delete.clone(),
			series_pre_insert: self.series_pre_insert.clone(),
			series_post_insert: self.series_post_insert.clone(),
			series_pre_update: self.series_pre_update.clone(),
			series_post_update: self.series_post_update.clone(),
			series_pre_delete: self.series_pre_delete.clone(),
			series_post_delete: self.series_post_delete.clone(),
			series_def_post_create: self.series_def_post_create.clone(),
			series_def_pre_update: self.series_def_pre_update.clone(),
			series_def_post_update: self.series_def_post_update.clone(),
			series_def_pre_delete: self.series_def_pre_delete.clone(),
			identity_def_post_create: self.identity_def_post_create.clone(),
			identity_def_pre_update: self.identity_def_pre_update.clone(),
			identity_def_post_update: self.identity_def_post_update.clone(),
			identity_def_pre_delete: self.identity_def_pre_delete.clone(),
			role_def_post_create: self.role_def_post_create.clone(),
			role_def_pre_update: self.role_def_pre_update.clone(),
			role_def_post_update: self.role_def_post_update.clone(),
			role_def_pre_delete: self.role_def_pre_delete.clone(),
			identity_role_def_post_create: self.identity_role_def_post_create.clone(),
			identity_role_def_pre_delete: self.identity_role_def_pre_delete.clone(),
			authentication_def_post_create: self.authentication_def_post_create.clone(),
			authentication_def_pre_delete: self.authentication_def_pre_delete.clone(),
		}
	}
}
