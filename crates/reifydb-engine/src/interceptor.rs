// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	interceptor::{
		InterceptorFactory, Interceptors, PostCommitInterceptor,
		PreCommitInterceptor, TablePostDeleteInterceptor,
		TablePostInsertInterceptor, TablePostUpdateInterceptor,
		TablePreDeleteInterceptor, TablePreInsertInterceptor,
		TablePreUpdateInterceptor,
	},
	interface::Transaction,
};

/// Builder for configuring interceptors
pub struct InterceptorBuilder<T: Transaction> {
	table_pre_insert: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePreInsertInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_post_insert: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePostInsertInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_pre_update: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePreUpdateInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_post_update: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePostUpdateInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_pre_delete: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePreDeleteInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_post_delete: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePostDeleteInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	pre_commit: Vec<
		Arc<
			dyn Fn() -> Box<dyn PreCommitInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	post_commit: Vec<
		Arc<
			dyn Fn() -> Box<dyn PostCommitInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
}

impl<T: Transaction> Default for InterceptorBuilder<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> InterceptorBuilder<T> {
	pub fn new() -> Self {
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

	pub fn add_table_pre_insert<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePreInsertInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_pre_insert.push(Arc::new(factory));
		self
	}

	pub fn add_table_post_insert<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePostInsertInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_post_insert.push(Arc::new(factory));
		self
	}

	pub fn add_table_pre_update<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePreUpdateInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_pre_update.push(Arc::new(factory));
		self
	}

	pub fn add_table_post_update<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePostUpdateInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_post_update.push(Arc::new(factory));
		self
	}

	pub fn add_table_pre_delete<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePreDeleteInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_pre_delete.push(Arc::new(factory));
		self
	}

	pub fn add_table_post_delete<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePostDeleteInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.table_post_delete.push(Arc::new(factory));
		self
	}

	pub fn add_pre_commit<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn PreCommitInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.pre_commit.push(Arc::new(factory));
		self
	}

	pub fn add_post_commit<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn PostCommitInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.post_commit.push(Arc::new(factory));
		self
	}

	pub fn build(self) -> StandardInterceptorFactory<T> {
		StandardInterceptorFactory {
			table_pre_insert: self.table_pre_insert,
			table_post_insert: self.table_post_insert,
			table_pre_update: self.table_pre_update,
			table_post_update: self.table_post_update,
			table_pre_delete: self.table_pre_delete,
			table_post_delete: self.table_post_delete,
			pre_commit: self.pre_commit,
			post_commit: self.post_commit,
		}
	}
}

/// Standard implementation of InterceptorFactory
pub struct StandardInterceptorFactory<T: Transaction> {
	table_pre_insert: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePreInsertInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_post_insert: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePostInsertInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_pre_update: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePreUpdateInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_post_update: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePostUpdateInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_pre_delete: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePreDeleteInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	table_post_delete: Vec<
		Arc<
			dyn Fn() -> Box<dyn TablePostDeleteInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	pre_commit: Vec<
		Arc<
			dyn Fn() -> Box<dyn PreCommitInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
	post_commit: Vec<
		Arc<
			dyn Fn() -> Box<dyn PostCommitInterceptor<T>>
				+ Send
				+ Sync,
		>,
	>,
}

impl<T: Transaction> Default for StandardInterceptorFactory<T> {
	fn default() -> Self {
		Self {
			table_pre_insert: vec![],
			table_post_insert: vec![],
			table_pre_update: vec![],
			table_post_update: vec![],
			table_pre_delete: vec![],
			table_post_delete: vec![],
			pre_commit: vec![],
			post_commit: vec![],
		}
	}
}

impl<T: Transaction> InterceptorFactory<T> for StandardInterceptorFactory<T> {
	fn create(&self) -> Interceptors<T> {
		let mut interceptors = Interceptors::new();

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
