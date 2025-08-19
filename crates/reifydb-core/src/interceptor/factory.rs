// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use crate::{
	interceptor::{
		Interceptors, PostCommitInterceptor, PreCommitInterceptor,
		TablePostDeleteInterceptor, TablePostInsertInterceptor,
		TablePostUpdateInterceptor, TablePreDeleteInterceptor,
		TablePreInsertInterceptor, TablePreUpdateInterceptor,
	},
	interface::Transaction,
};

/// Factory trait for creating interceptor instances for each transaction
pub trait InterceptorFactory<T: Transaction>: Send + Sync {
	/// Create a new instance of interceptors for a transaction
	fn create(&self) -> Interceptors<T>;
}

/// Standard implementation of InterceptorFactory that stores interceptors in a
/// Send+Sync safe way
pub struct StandardInterceptorFactory<T: Transaction> {
	pub(crate) table_pre_insert: Vec<Arc<dyn TablePreInsertInterceptor<T>>>,
	pub(crate) table_post_insert:
		Vec<Arc<dyn TablePostInsertInterceptor<T>>>,
	pub(crate) table_pre_update: Vec<Arc<dyn TablePreUpdateInterceptor<T>>>,
	pub(crate) table_post_update:
		Vec<Arc<dyn TablePostUpdateInterceptor<T>>>,
	pub(crate) table_pre_delete: Vec<Arc<dyn TablePreDeleteInterceptor<T>>>,
	pub(crate) table_post_delete:
		Vec<Arc<dyn TablePostDeleteInterceptor<T>>>,
	pub(crate) pre_commit: Vec<Arc<dyn PreCommitInterceptor<T>>>,
	pub(crate) post_commit: Vec<Arc<dyn PostCommitInterceptor<T>>>,
}

impl<T: Transaction> Default for StandardInterceptorFactory<T> {
	fn default() -> Self {
		Self {
			table_pre_insert: Vec::new(),
			table_post_insert: Vec::new(),
			table_pre_update: Vec::new(),
			table_post_update: Vec::new(),
			table_pre_delete: Vec::new(),
			table_post_delete: Vec::new(),
			pre_commit: Vec::new(),
			post_commit: Vec::new(),
		}
	}
}

impl<T: Transaction> InterceptorFactory<T> for StandardInterceptorFactory<T> {
	fn create(&self) -> Interceptors<T> {
		let mut interceptors = Interceptors::new();

		// Clone the Arc references into the new Interceptors instance
		for interceptor in &self.table_pre_insert {
			interceptors
				.add_table_pre_insert(Arc::clone(interceptor));
		}
		for interceptor in &self.table_post_insert {
			interceptors
				.add_table_post_insert(Arc::clone(interceptor));
		}
		for interceptor in &self.table_pre_update {
			interceptors
				.add_table_pre_update(Arc::clone(interceptor));
		}
		for interceptor in &self.table_post_update {
			interceptors
				.add_table_post_update(Arc::clone(interceptor));
		}
		for interceptor in &self.table_pre_delete {
			interceptors
				.add_table_pre_delete(Arc::clone(interceptor));
		}
		for interceptor in &self.table_post_delete {
			interceptors
				.add_table_post_delete(Arc::clone(interceptor));
		}
		for interceptor in &self.pre_commit {
			interceptors.add_pre_commit(Arc::clone(interceptor));
		}
		for interceptor in &self.post_commit {
			interceptors.add_post_commit(Arc::clone(interceptor));
		}

		interceptors
	}
}
