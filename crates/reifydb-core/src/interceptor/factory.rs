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

/// Factory function that creates an interceptor instance
type InterceptorFactoryFn<I> = Arc<dyn Fn() -> Arc<I> + Send + Sync>;

/// Standard implementation of InterceptorFactory that stores factory functions
/// This allows the factory to be Send+Sync while creating non-Send/Sync
/// interceptors
pub struct StandardInterceptorFactory<T: Transaction> {
	pub(crate) table_pre_insert:
		Vec<InterceptorFactoryFn<dyn TablePreInsertInterceptor<T>>>,
	pub(crate) table_post_insert:
		Vec<InterceptorFactoryFn<dyn TablePostInsertInterceptor<T>>>,
	pub(crate) table_pre_update:
		Vec<InterceptorFactoryFn<dyn TablePreUpdateInterceptor<T>>>,
	pub(crate) table_post_update:
		Vec<InterceptorFactoryFn<dyn TablePostUpdateInterceptor<T>>>,
	pub(crate) table_pre_delete:
		Vec<InterceptorFactoryFn<dyn TablePreDeleteInterceptor<T>>>,
	pub(crate) table_post_delete:
		Vec<InterceptorFactoryFn<dyn TablePostDeleteInterceptor<T>>>,
	pub(crate) pre_commit:
		Vec<InterceptorFactoryFn<dyn PreCommitInterceptor<T>>>,
	pub(crate) post_commit:
		Vec<InterceptorFactoryFn<dyn PostCommitInterceptor<T>>>,
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

impl<T: Transaction> StandardInterceptorFactory<T> {
	/// Add a factory function for a table pre-insert interceptor
	pub fn add_table_pre_insert_factory<F>(&mut self, factory: F)
	where
		F: Fn() -> Arc<dyn TablePreInsertInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_pre_insert.push(Arc::new(factory));
	}

	/// Add a factory function for a table post-insert interceptor
	pub fn add_table_post_insert_factory<F>(&mut self, factory: F)
	where
		F: Fn() -> Arc<dyn TablePostInsertInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_post_insert.push(Arc::new(factory));
	}

	/// Add a factory function for a table pre-update interceptor
	pub fn add_table_pre_update_factory<F>(&mut self, factory: F)
	where
		F: Fn() -> Arc<dyn TablePreUpdateInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_pre_update.push(Arc::new(factory));
	}

	/// Add a factory function for a table post-update interceptor
	pub fn add_table_post_update_factory<F>(&mut self, factory: F)
	where
		F: Fn() -> Arc<dyn TablePostUpdateInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_post_update.push(Arc::new(factory));
	}

	/// Add a factory function for a table pre-delete interceptor
	pub fn add_table_pre_delete_factory<F>(&mut self, factory: F)
	where
		F: Fn() -> Arc<dyn TablePreDeleteInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_pre_delete.push(Arc::new(factory));
	}

	/// Add a factory function for a table post-delete interceptor
	pub fn add_table_post_delete_factory<F>(&mut self, factory: F)
	where
		F: Fn() -> Arc<dyn TablePostDeleteInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_post_delete.push(Arc::new(factory));
	}

	/// Add a factory function for a pre-commit interceptor
	pub fn add_pre_commit_factory<F>(&mut self, factory: F)
	where
		F: Fn() -> Arc<dyn PreCommitInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.pre_commit.push(Arc::new(factory));
	}

	/// Add a factory function for a post-commit interceptor
	pub fn add_post_commit_factory<F>(&mut self, factory: F)
	where
		F: Fn() -> Arc<dyn PostCommitInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.post_commit.push(Arc::new(factory));
	}
}

impl<T: Transaction> InterceptorFactory<T> for StandardInterceptorFactory<T> {
	fn create(&self) -> Interceptors<T> {
		let mut interceptors = Interceptors::new();

		// Create new interceptor instances using the factory functions
		for factory in &self.table_pre_insert {
			interceptors.add_table_pre_insert(factory());
		}
		for factory in &self.table_post_insert {
			interceptors.add_table_post_insert(factory());
		}
		for factory in &self.table_pre_update {
			interceptors.add_table_pre_update(factory());
		}
		for factory in &self.table_post_update {
			interceptors.add_table_post_update(factory());
		}
		for factory in &self.table_pre_delete {
			interceptors.add_table_pre_delete(factory());
		}
		for factory in &self.table_post_delete {
			interceptors.add_table_post_delete(factory());
		}
		for factory in &self.pre_commit {
			interceptors.add_pre_commit(factory());
		}
		for factory in &self.post_commit {
			interceptors.add_post_commit(factory());
		}

		interceptors
	}
}
