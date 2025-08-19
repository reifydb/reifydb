// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use crate::{
	interceptor::{
		Chain, InterceptorChain, PostCommitInterceptor,
		PreCommitInterceptor, TablePostDeleteInterceptor,
		TablePostInsertInterceptor, TablePostUpdateInterceptor,
		TablePreDeleteInterceptor, TablePreInsertInterceptor,
		TablePreUpdateInterceptor,
	},
	interface::Transaction,
};

/// Container for all interceptor chains
pub struct Interceptors<T: Transaction> {
	pub table_pre_insert: Chain<T, dyn TablePreInsertInterceptor<T>>,
	pub table_post_insert: Chain<T, dyn TablePostInsertInterceptor<T>>,
	pub table_pre_update: Chain<T, dyn TablePreUpdateInterceptor<T>>,
	pub table_post_update: Chain<T, dyn TablePostUpdateInterceptor<T>>,
	pub table_pre_delete: Chain<T, dyn TablePreDeleteInterceptor<T>>,
	pub table_post_delete: Chain<T, dyn TablePostDeleteInterceptor<T>>,
	pub pre_commit: Chain<T, dyn PreCommitInterceptor<T>>,
	pub post_commit: Chain<T, dyn PostCommitInterceptor<T>>,
	// Marker to prevent Send and Sync
	_not_send_sync: PhantomData<*const ()>,
}

impl<T: Transaction> Default for Interceptors<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> Interceptors<T> {
	pub fn new() -> Self {
		Self {
			table_pre_insert: InterceptorChain::new(),
			table_post_insert: InterceptorChain::new(),
			table_pre_update: InterceptorChain::new(),
			table_post_update: InterceptorChain::new(),
			table_pre_delete: InterceptorChain::new(),
			table_post_delete: InterceptorChain::new(),
			pre_commit: InterceptorChain::new(),
			post_commit: InterceptorChain::new(),
			_not_send_sync: PhantomData,
		}
	}
}

impl<T: Transaction> Clone for Interceptors<T> {
	fn clone(&self) -> Self {
		Self {
			table_pre_insert: self.table_pre_insert.clone(),
			table_post_insert: self.table_post_insert.clone(),
			table_pre_update: self.table_pre_update.clone(),
			table_post_update: self.table_post_update.clone(),
			table_pre_delete: self.table_pre_delete.clone(),
			table_post_delete: self.table_post_delete.clone(),
			pre_commit: self.pre_commit.clone(),
			post_commit: self.post_commit.clone(),
			_not_send_sync: PhantomData,
		}
	}
}

impl<T: Transaction> Interceptors<T> {
	/// Add a pre-insert interceptor
	pub fn add_table_pre_insert(
		&mut self,
		interceptor: Arc<dyn TablePreInsertInterceptor<T>>,
	) {
		self.table_pre_insert.add(interceptor);
	}

	/// Add a post-insert interceptor
	pub fn add_table_post_insert(
		&mut self,
		interceptor: Arc<dyn TablePostInsertInterceptor<T>>,
	) {
		self.table_post_insert.add(interceptor);
	}

	/// Add a pre-update interceptor
	pub fn add_table_pre_update(
		&mut self,
		interceptor: Arc<dyn TablePreUpdateInterceptor<T>>,
	) {
		self.table_pre_update.add(interceptor);
	}

	/// Add a post-update interceptor
	pub fn add_table_post_update(
		&mut self,
		interceptor: Arc<dyn TablePostUpdateInterceptor<T>>,
	) {
		self.table_post_update.add(interceptor);
	}

	/// Add a pre-delete interceptor
	pub fn add_table_pre_delete(
		&mut self,
		interceptor: Arc<dyn TablePreDeleteInterceptor<T>>,
	) {
		self.table_pre_delete.add(interceptor);
	}

	/// Add a post-delete interceptor
	pub fn add_table_post_delete(
		&mut self,
		interceptor: Arc<dyn TablePostDeleteInterceptor<T>>,
	) {
		self.table_post_delete.add(interceptor);
	}

	/// Add a pre-commit interceptor
	pub fn add_pre_commit(
		&mut self,
		interceptor: Arc<dyn PreCommitInterceptor<T>>,
	) {
		self.pre_commit.add(interceptor);
	}

	/// Add a post-commit interceptor
	pub fn add_post_commit(
		&mut self,
		interceptor: Arc<dyn PostCommitInterceptor<T>>,
	) {
		self.post_commit.add(interceptor);
	}
}
