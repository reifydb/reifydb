// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_type::Result;

use super::{
	authentication::{AuthenticationPostCreateInterceptor, AuthenticationPreDeleteInterceptor},
	chain::InterceptorChain,
	dictionary::{
		DictionaryPostCreateInterceptor, DictionaryPostUpdateInterceptor, DictionaryPreDeleteInterceptor,
		DictionaryPreUpdateInterceptor,
	},
	dictionary_row::{
		DictionaryRowPostDeleteInterceptor, DictionaryRowPostInsertInterceptor,
		DictionaryRowPostUpdateInterceptor, DictionaryRowPreDeleteInterceptor,
		DictionaryRowPreInsertInterceptor, DictionaryRowPreUpdateInterceptor,
	},
	granted_role::{GrantedRolePostCreateInterceptor, GrantedRolePreDeleteInterceptor},
	identity::{
		IdentityPostCreateInterceptor, IdentityPostUpdateInterceptor, IdentityPreDeleteInterceptor,
		IdentityPreUpdateInterceptor,
	},
	namespace::{
		NamespacePostCreateInterceptor, NamespacePostUpdateInterceptor, NamespacePreDeleteInterceptor,
		NamespacePreUpdateInterceptor,
	},
	ringbuffer::{
		RingBufferPostCreateInterceptor, RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor,
		RingBufferPreUpdateInterceptor,
	},
	ringbuffer_row::{
		RingBufferRowPostDeleteInterceptor, RingBufferRowPostInsertInterceptor,
		RingBufferRowPostUpdateInterceptor, RingBufferRowPreDeleteInterceptor,
		RingBufferRowPreInsertInterceptor, RingBufferRowPreUpdateInterceptor,
	},
	role::{
		RolePostCreateInterceptor, RolePostUpdateInterceptor, RolePreDeleteInterceptor,
		RolePreUpdateInterceptor,
	},
	series::{
		SeriesPostCreateInterceptor, SeriesPostUpdateInterceptor, SeriesPreDeleteInterceptor,
		SeriesPreUpdateInterceptor,
	},
	series_row::{
		SeriesRowPostDeleteInterceptor, SeriesRowPostInsertInterceptor, SeriesRowPostUpdateInterceptor,
		SeriesRowPreDeleteInterceptor, SeriesRowPreInsertInterceptor, SeriesRowPreUpdateInterceptor,
	},
	table::{
		TablePostCreateInterceptor, TablePostUpdateInterceptor, TablePreDeleteInterceptor,
		TablePreUpdateInterceptor,
	},
	table_row::{
		TableRowPostDeleteInterceptor, TableRowPostInsertInterceptor, TableRowPostUpdateInterceptor,
		TableRowPreDeleteInterceptor, TableRowPreInsertInterceptor, TableRowPreUpdateInterceptor,
	},
	transaction::{PostCommitInterceptor, PreCommitInterceptor},
	view::{
		ViewPostCreateInterceptor, ViewPostUpdateInterceptor, ViewPreDeleteInterceptor,
		ViewPreUpdateInterceptor,
	},
	view_row::{
		ViewRowPostDeleteInterceptor, ViewRowPostInsertInterceptor, ViewRowPostUpdateInterceptor,
		ViewRowPreDeleteInterceptor, ViewRowPreInsertInterceptor, ViewRowPreUpdateInterceptor,
	},
};
use crate::transaction::TestTransaction;

pub type Chain<I> = InterceptorChain<I>;
type TestPreCommitHook = Arc<dyn Fn(&mut TestTransaction<'_>) -> Result<()> + Send + Sync>;

pub struct Interceptors {
	pub table_row_pre_insert: Chain<dyn TableRowPreInsertInterceptor + Send + Sync>,
	pub table_row_post_insert: Chain<dyn TableRowPostInsertInterceptor + Send + Sync>,
	pub table_row_pre_update: Chain<dyn TableRowPreUpdateInterceptor + Send + Sync>,
	pub table_row_post_update: Chain<dyn TableRowPostUpdateInterceptor + Send + Sync>,
	pub table_row_pre_delete: Chain<dyn TableRowPreDeleteInterceptor + Send + Sync>,
	pub table_row_post_delete: Chain<dyn TableRowPostDeleteInterceptor + Send + Sync>,
	pub ringbuffer_row_pre_insert: Chain<dyn RingBufferRowPreInsertInterceptor + Send + Sync>,
	pub ringbuffer_row_post_insert: Chain<dyn RingBufferRowPostInsertInterceptor + Send + Sync>,
	pub ringbuffer_row_pre_update: Chain<dyn RingBufferRowPreUpdateInterceptor + Send + Sync>,
	pub ringbuffer_row_post_update: Chain<dyn RingBufferRowPostUpdateInterceptor + Send + Sync>,
	pub ringbuffer_row_pre_delete: Chain<dyn RingBufferRowPreDeleteInterceptor + Send + Sync>,
	pub ringbuffer_row_post_delete: Chain<dyn RingBufferRowPostDeleteInterceptor + Send + Sync>,
	pub pre_commit: Chain<dyn PreCommitInterceptor + Send + Sync>,
	pub post_commit: Chain<dyn PostCommitInterceptor + Send + Sync>,
	pub namespace_post_create: Chain<dyn NamespacePostCreateInterceptor + Send + Sync>,
	pub namespace_pre_update: Chain<dyn NamespacePreUpdateInterceptor + Send + Sync>,
	pub namespace_post_update: Chain<dyn NamespacePostUpdateInterceptor + Send + Sync>,
	pub namespace_pre_delete: Chain<dyn NamespacePreDeleteInterceptor + Send + Sync>,
	pub table_post_create: Chain<dyn TablePostCreateInterceptor + Send + Sync>,
	pub table_pre_update: Chain<dyn TablePreUpdateInterceptor + Send + Sync>,
	pub table_post_update: Chain<dyn TablePostUpdateInterceptor + Send + Sync>,
	pub table_pre_delete: Chain<dyn TablePreDeleteInterceptor + Send + Sync>,
	pub view_row_pre_insert: Chain<dyn ViewRowPreInsertInterceptor + Send + Sync>,
	pub view_row_post_insert: Chain<dyn ViewRowPostInsertInterceptor + Send + Sync>,
	pub view_row_pre_update: Chain<dyn ViewRowPreUpdateInterceptor + Send + Sync>,
	pub view_row_post_update: Chain<dyn ViewRowPostUpdateInterceptor + Send + Sync>,
	pub view_row_pre_delete: Chain<dyn ViewRowPreDeleteInterceptor + Send + Sync>,
	pub view_row_post_delete: Chain<dyn ViewRowPostDeleteInterceptor + Send + Sync>,
	pub view_post_create: Chain<dyn ViewPostCreateInterceptor + Send + Sync>,
	pub view_pre_update: Chain<dyn ViewPreUpdateInterceptor + Send + Sync>,
	pub view_post_update: Chain<dyn ViewPostUpdateInterceptor + Send + Sync>,
	pub view_pre_delete: Chain<dyn ViewPreDeleteInterceptor + Send + Sync>,
	pub ringbuffer_post_create: Chain<dyn RingBufferPostCreateInterceptor + Send + Sync>,
	pub ringbuffer_pre_update: Chain<dyn RingBufferPreUpdateInterceptor + Send + Sync>,
	pub ringbuffer_post_update: Chain<dyn RingBufferPostUpdateInterceptor + Send + Sync>,
	pub ringbuffer_pre_delete: Chain<dyn RingBufferPreDeleteInterceptor + Send + Sync>,
	pub dictionary_row_pre_insert: Chain<dyn DictionaryRowPreInsertInterceptor + Send + Sync>,
	pub dictionary_row_post_insert: Chain<dyn DictionaryRowPostInsertInterceptor + Send + Sync>,
	pub dictionary_row_pre_update: Chain<dyn DictionaryRowPreUpdateInterceptor + Send + Sync>,
	pub dictionary_row_post_update: Chain<dyn DictionaryRowPostUpdateInterceptor + Send + Sync>,
	pub dictionary_row_pre_delete: Chain<dyn DictionaryRowPreDeleteInterceptor + Send + Sync>,
	pub dictionary_row_post_delete: Chain<dyn DictionaryRowPostDeleteInterceptor + Send + Sync>,
	pub dictionary_post_create: Chain<dyn DictionaryPostCreateInterceptor + Send + Sync>,
	pub dictionary_pre_update: Chain<dyn DictionaryPreUpdateInterceptor + Send + Sync>,
	pub dictionary_post_update: Chain<dyn DictionaryPostUpdateInterceptor + Send + Sync>,
	pub dictionary_pre_delete: Chain<dyn DictionaryPreDeleteInterceptor + Send + Sync>,
	pub series_row_pre_insert: Chain<dyn SeriesRowPreInsertInterceptor + Send + Sync>,
	pub series_row_post_insert: Chain<dyn SeriesRowPostInsertInterceptor + Send + Sync>,
	pub series_row_pre_update: Chain<dyn SeriesRowPreUpdateInterceptor + Send + Sync>,
	pub series_row_post_update: Chain<dyn SeriesRowPostUpdateInterceptor + Send + Sync>,
	pub series_row_pre_delete: Chain<dyn SeriesRowPreDeleteInterceptor + Send + Sync>,
	pub series_row_post_delete: Chain<dyn SeriesRowPostDeleteInterceptor + Send + Sync>,
	pub series_post_create: Chain<dyn SeriesPostCreateInterceptor + Send + Sync>,
	pub series_pre_update: Chain<dyn SeriesPreUpdateInterceptor + Send + Sync>,
	pub series_post_update: Chain<dyn SeriesPostUpdateInterceptor + Send + Sync>,
	pub series_pre_delete: Chain<dyn SeriesPreDeleteInterceptor + Send + Sync>,
	pub identity_post_create: Chain<dyn IdentityPostCreateInterceptor + Send + Sync>,
	pub identity_pre_update: Chain<dyn IdentityPreUpdateInterceptor + Send + Sync>,
	pub identity_post_update: Chain<dyn IdentityPostUpdateInterceptor + Send + Sync>,
	pub identity_pre_delete: Chain<dyn IdentityPreDeleteInterceptor + Send + Sync>,
	pub role_post_create: Chain<dyn RolePostCreateInterceptor + Send + Sync>,
	pub role_pre_update: Chain<dyn RolePreUpdateInterceptor + Send + Sync>,
	pub role_post_update: Chain<dyn RolePostUpdateInterceptor + Send + Sync>,
	pub role_pre_delete: Chain<dyn RolePreDeleteInterceptor + Send + Sync>,
	pub granted_role_post_create: Chain<dyn GrantedRolePostCreateInterceptor + Send + Sync>,
	pub granted_role_pre_delete: Chain<dyn GrantedRolePreDeleteInterceptor + Send + Sync>,
	pub authentication_post_create: Chain<dyn AuthenticationPostCreateInterceptor + Send + Sync>,
	pub authentication_pre_delete: Chain<dyn AuthenticationPreDeleteInterceptor + Send + Sync>,

	/// Optional hook for test flow processing. When set, `capture_testing_pre_commit`
	/// calls this to register uncommitted flows in the shared flow engine before
	/// running the pre-commit interceptor chain.
	///
	/// Use [`set_test_pre_commit`](Interceptors::set_test_pre_commit) to configure.
	pub(crate) test_pre_commit: Option<TestPreCommitHook>,
}

impl Default for Interceptors {
	fn default() -> Self {
		Self::new()
	}
}

impl Interceptors {
	pub fn new() -> Self {
		Self {
			table_row_pre_insert: InterceptorChain::new(),
			table_row_post_insert: InterceptorChain::new(),
			table_row_pre_update: InterceptorChain::new(),
			table_row_post_update: InterceptorChain::new(),
			table_row_pre_delete: InterceptorChain::new(),
			table_row_post_delete: InterceptorChain::new(),
			ringbuffer_row_pre_insert: InterceptorChain::new(),
			ringbuffer_row_post_insert: InterceptorChain::new(),
			ringbuffer_row_pre_update: InterceptorChain::new(),
			ringbuffer_row_post_update: InterceptorChain::new(),
			ringbuffer_row_pre_delete: InterceptorChain::new(),
			ringbuffer_row_post_delete: InterceptorChain::new(),
			pre_commit: InterceptorChain::new(),
			post_commit: InterceptorChain::new(),
			namespace_post_create: InterceptorChain::new(),
			namespace_pre_update: InterceptorChain::new(),
			namespace_post_update: InterceptorChain::new(),
			namespace_pre_delete: InterceptorChain::new(),
			table_post_create: InterceptorChain::new(),
			table_pre_update: InterceptorChain::new(),
			table_post_update: InterceptorChain::new(),
			table_pre_delete: InterceptorChain::new(),
			view_row_pre_insert: InterceptorChain::new(),
			view_row_post_insert: InterceptorChain::new(),
			view_row_pre_update: InterceptorChain::new(),
			view_row_post_update: InterceptorChain::new(),
			view_row_pre_delete: InterceptorChain::new(),
			view_row_post_delete: InterceptorChain::new(),
			view_post_create: InterceptorChain::new(),
			view_pre_update: InterceptorChain::new(),
			view_post_update: InterceptorChain::new(),
			view_pre_delete: InterceptorChain::new(),
			ringbuffer_post_create: InterceptorChain::new(),
			ringbuffer_pre_update: InterceptorChain::new(),
			ringbuffer_post_update: InterceptorChain::new(),
			ringbuffer_pre_delete: InterceptorChain::new(),
			dictionary_row_pre_insert: InterceptorChain::new(),
			dictionary_row_post_insert: InterceptorChain::new(),
			dictionary_row_pre_update: InterceptorChain::new(),
			dictionary_row_post_update: InterceptorChain::new(),
			dictionary_row_pre_delete: InterceptorChain::new(),
			dictionary_row_post_delete: InterceptorChain::new(),
			dictionary_post_create: InterceptorChain::new(),
			dictionary_pre_update: InterceptorChain::new(),
			dictionary_post_update: InterceptorChain::new(),
			dictionary_pre_delete: InterceptorChain::new(),
			series_row_pre_insert: InterceptorChain::new(),
			series_row_post_insert: InterceptorChain::new(),
			series_row_pre_update: InterceptorChain::new(),
			series_row_post_update: InterceptorChain::new(),
			series_row_pre_delete: InterceptorChain::new(),
			series_row_post_delete: InterceptorChain::new(),
			series_post_create: InterceptorChain::new(),
			series_pre_update: InterceptorChain::new(),
			series_post_update: InterceptorChain::new(),
			series_pre_delete: InterceptorChain::new(),
			identity_post_create: InterceptorChain::new(),
			identity_pre_update: InterceptorChain::new(),
			identity_post_update: InterceptorChain::new(),
			identity_pre_delete: InterceptorChain::new(),
			role_post_create: InterceptorChain::new(),
			role_pre_update: InterceptorChain::new(),
			role_post_update: InterceptorChain::new(),
			role_pre_delete: InterceptorChain::new(),
			granted_role_post_create: InterceptorChain::new(),
			granted_role_pre_delete: InterceptorChain::new(),
			authentication_post_create: InterceptorChain::new(),
			authentication_pre_delete: InterceptorChain::new(),
			test_pre_commit: None,
		}
	}

	/// Register a hook for test-only pre-commit flow processing.
	///
	/// This hook is called by [`TestTransaction::capture_testing_pre_commit`] to
	/// rebuild the shared flow engine from all catalog flows (including uncommitted
	/// ones) before the pre-commit interceptor chain runs.
	pub fn set_test_pre_commit(&mut self, hook: TestPreCommitHook) {
		self.test_pre_commit = Some(hook);
	}
}

pub trait RegisterInterceptor: Send + Sync {
	fn register(self, interceptors: &mut Interceptors);
}

impl Clone for Interceptors {
	fn clone(&self) -> Self {
		Self {
			table_row_pre_insert: self.table_row_pre_insert.clone(),
			table_row_post_insert: self.table_row_post_insert.clone(),
			table_row_pre_update: self.table_row_pre_update.clone(),
			table_row_post_update: self.table_row_post_update.clone(),
			table_row_pre_delete: self.table_row_pre_delete.clone(),
			table_row_post_delete: self.table_row_post_delete.clone(),
			ringbuffer_row_pre_insert: self.ringbuffer_row_pre_insert.clone(),
			ringbuffer_row_post_insert: self.ringbuffer_row_post_insert.clone(),
			ringbuffer_row_pre_update: self.ringbuffer_row_pre_update.clone(),
			ringbuffer_row_post_update: self.ringbuffer_row_post_update.clone(),
			ringbuffer_row_pre_delete: self.ringbuffer_row_pre_delete.clone(),
			ringbuffer_row_post_delete: self.ringbuffer_row_post_delete.clone(),
			pre_commit: self.pre_commit.clone(),
			post_commit: self.post_commit.clone(),
			namespace_post_create: self.namespace_post_create.clone(),
			namespace_pre_update: self.namespace_pre_update.clone(),
			namespace_post_update: self.namespace_post_update.clone(),
			namespace_pre_delete: self.namespace_pre_delete.clone(),
			table_post_create: self.table_post_create.clone(),
			table_pre_update: self.table_pre_update.clone(),
			table_post_update: self.table_post_update.clone(),
			table_pre_delete: self.table_pre_delete.clone(),
			view_row_pre_insert: self.view_row_pre_insert.clone(),
			view_row_post_insert: self.view_row_post_insert.clone(),
			view_row_pre_update: self.view_row_pre_update.clone(),
			view_row_post_update: self.view_row_post_update.clone(),
			view_row_pre_delete: self.view_row_pre_delete.clone(),
			view_row_post_delete: self.view_row_post_delete.clone(),
			view_post_create: self.view_post_create.clone(),
			view_pre_update: self.view_pre_update.clone(),
			view_post_update: self.view_post_update.clone(),
			view_pre_delete: self.view_pre_delete.clone(),
			ringbuffer_post_create: self.ringbuffer_post_create.clone(),
			ringbuffer_pre_update: self.ringbuffer_pre_update.clone(),
			ringbuffer_post_update: self.ringbuffer_post_update.clone(),
			ringbuffer_pre_delete: self.ringbuffer_pre_delete.clone(),
			dictionary_row_pre_insert: self.dictionary_row_pre_insert.clone(),
			dictionary_row_post_insert: self.dictionary_row_post_insert.clone(),
			dictionary_row_pre_update: self.dictionary_row_pre_update.clone(),
			dictionary_row_post_update: self.dictionary_row_post_update.clone(),
			dictionary_row_pre_delete: self.dictionary_row_pre_delete.clone(),
			dictionary_row_post_delete: self.dictionary_row_post_delete.clone(),
			dictionary_post_create: self.dictionary_post_create.clone(),
			dictionary_pre_update: self.dictionary_pre_update.clone(),
			dictionary_post_update: self.dictionary_post_update.clone(),
			dictionary_pre_delete: self.dictionary_pre_delete.clone(),
			series_row_pre_insert: self.series_row_pre_insert.clone(),
			series_row_post_insert: self.series_row_post_insert.clone(),
			series_row_pre_update: self.series_row_pre_update.clone(),
			series_row_post_update: self.series_row_post_update.clone(),
			series_row_pre_delete: self.series_row_pre_delete.clone(),
			series_row_post_delete: self.series_row_post_delete.clone(),
			series_post_create: self.series_post_create.clone(),
			series_pre_update: self.series_pre_update.clone(),
			series_post_update: self.series_post_update.clone(),
			series_pre_delete: self.series_pre_delete.clone(),
			identity_post_create: self.identity_post_create.clone(),
			identity_pre_update: self.identity_pre_update.clone(),
			identity_post_update: self.identity_post_update.clone(),
			identity_pre_delete: self.identity_pre_delete.clone(),
			role_post_create: self.role_post_create.clone(),
			role_pre_update: self.role_pre_update.clone(),
			role_post_update: self.role_post_update.clone(),
			role_pre_delete: self.role_pre_delete.clone(),
			granted_role_post_create: self.granted_role_post_create.clone(),
			granted_role_pre_delete: self.granted_role_pre_delete.clone(),
			authentication_post_create: self.authentication_post_create.clone(),
			authentication_pre_delete: self.authentication_pre_delete.clone(),
			test_pre_commit: self.test_pre_commit.clone(),
		}
	}
}
