// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::Transaction;

pub mod chain;
pub mod table;
pub mod transaction;

pub use chain::InterceptorChain;
pub use table::*;
pub use transaction::{PostCommitInterceptor, PreCommitInterceptor};

/// Container for all interceptor chains
pub struct Interceptors<T: Transaction> {
	pub table_pre_insert:
		InterceptorChain<T, dyn TablePreInsertInterceptor<T>>,
	pub table_post_insert:
		InterceptorChain<T, dyn TablePostInsertInterceptor<T>>,
	pub table_pre_update:
		InterceptorChain<T, dyn TablePreUpdateInterceptor<T>>,
	pub table_post_update:
		InterceptorChain<T, dyn TablePostUpdateInterceptor<T>>,
	pub table_pre_delete:
		InterceptorChain<T, dyn TablePreDeleteInterceptor<T>>,
	pub table_post_delete:
		InterceptorChain<T, dyn TablePostDeleteInterceptor<T>>,
	pub pre_commit: InterceptorChain<T, dyn PreCommitInterceptor<T>>,
	pub post_commit: InterceptorChain<T, dyn PostCommitInterceptor<T>>,
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
		}
	}

	/// Add a pre-insert interceptor
	pub fn add_table_pre_insert(
		&mut self,
		interceptor: Box<dyn TablePreInsertInterceptor<T>>,
	) {
		self.table_pre_insert.add(interceptor);
	}

	/// Add a post-insert interceptor
	pub fn add_table_post_insert(
		&mut self,
		interceptor: Box<dyn TablePostInsertInterceptor<T>>,
	) {
		self.table_post_insert.add(interceptor);
	}

	/// Add a pre-update interceptor
	pub fn add_table_pre_update(
		&mut self,
		interceptor: Box<dyn TablePreUpdateInterceptor<T>>,
	) {
		self.table_pre_update.add(interceptor);
	}

	/// Add a post-update interceptor
	pub fn add_table_post_update(
		&mut self,
		interceptor: Box<dyn TablePostUpdateInterceptor<T>>,
	) {
		self.table_post_update.add(interceptor);
	}

	/// Add a pre-delete interceptor
	pub fn add_table_pre_delete(
		&mut self,
		interceptor: Box<dyn TablePreDeleteInterceptor<T>>,
	) {
		self.table_pre_delete.add(interceptor);
	}

	/// Add a post-delete interceptor
	pub fn add_table_post_delete(
		&mut self,
		interceptor: Box<dyn TablePostDeleteInterceptor<T>>,
	) {
		self.table_post_delete.add(interceptor);
	}

	/// Add a pre-commit interceptor
	pub fn add_pre_commit(
		&mut self,
		interceptor: Box<dyn PreCommitInterceptor<T>>,
	) {
		self.pre_commit.add(interceptor);
	}

	/// Add a post-commit interceptor
	pub fn add_post_commit(
		&mut self,
		interceptor: Box<dyn PostCommitInterceptor<T>>,
	) {
		self.post_commit.add(interceptor);
	}
}
