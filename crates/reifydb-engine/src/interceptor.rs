// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later, see license.md file
// 
// use std::sync::Arc;
// 
// use reifydb_core::{
// 	interceptor::{
// 		AddToBuilder, InterceptorFactory, Interceptors,
// 		PostCommitInterceptor, PreCommitInterceptor,
// 		TablePostDeleteInterceptor, TablePostInsertInterceptor,
// 		TablePostUpdateInterceptor, TablePreDeleteInterceptor,
// 		TablePreInsertInterceptor, TablePreUpdateInterceptor,
// 	},
// 	interface::Transaction,
// };
// 
// /// Builder for configuring interceptors
// pub struct InterceptorBuilder<T: Transaction> {
// 	table_pre_insert: Vec<Arc<dyn TablePreInsertInterceptor<T>>>,
// 	table_post_insert: Vec<Arc<dyn TablePostInsertInterceptor<T>>>,
// 	table_pre_update: Vec<Arc<dyn TablePreUpdateInterceptor<T>>>,
// 	table_post_update: Vec<Arc<dyn TablePostUpdateInterceptor<T>>>,
// 	table_pre_delete: Vec<Arc<dyn TablePreDeleteInterceptor<T>>>,
// 	table_post_delete: Vec<Arc<dyn TablePostDeleteInterceptor<T>>>,
// 	pre_commit: Vec<Arc<dyn PreCommitInterceptor<T>>>,
// 	post_commit: Vec<Arc<dyn PostCommitInterceptor<T>>>,
// }
// 
// impl<T: Transaction> Default for InterceptorBuilder<T> {
// 	fn default() -> Self {
// 		Self::new()
// 	}
// }
// 
// impl<T: Transaction> InterceptorBuilder<T> {
// 	pub fn new() -> Self {
// 		Self {
// 			table_pre_insert: Vec::new(),
// 			table_post_insert: Vec::new(),
// 			table_pre_update: Vec::new(),
// 			table_post_update: Vec::new(),
// 			table_pre_delete: Vec::new(),
// 			table_post_delete: Vec::new(),
// 			pre_commit: Vec::new(),
// 			post_commit: Vec::new(),
// 		}
// 	}
// 
// 	/// Add any interceptor - the type determines which chain it goes to
// 	pub fn add_interceptor<I>(self, interceptor: I) -> Self
// 	where
// 		I: AddToBuilder<T>,
// 	{
// 		interceptor.add_to_builder(self)
// 	}
// 
// 	pub fn add_table_pre_insert<I>(mut self, interceptor: I) -> Self
// 	where
// 		I: TablePreInsertInterceptor<T> + 'static,
// 	{
// 		self.table_pre_insert.push(Arc::new(interceptor));
// 		self
// 	}
// 
// 	pub fn add_table_post_insert<I>(mut self, interceptor: I) -> Self
// 	where
// 		I: TablePostInsertInterceptor<T> + 'static,
// 	{
// 		self.table_post_insert.push(Arc::new(interceptor));
// 		self
// 	}
// 
// 	pub fn add_table_pre_update<I>(mut self, interceptor: I) -> Self
// 	where
// 		I: TablePreUpdateInterceptor<T> + 'static,
// 	{
// 		self.table_pre_update.push(Arc::new(interceptor));
// 		self
// 	}
// 
// 	pub fn add_table_post_update<I>(mut self, interceptor: I) -> Self
// 	where
// 		I: TablePostUpdateInterceptor<T> + 'static,
// 	{
// 		self.table_post_update.push(Arc::new(interceptor));
// 		self
// 	}
// 
// 	pub fn add_table_pre_delete<I>(mut self, interceptor: I) -> Self
// 	where
// 		I: TablePreDeleteInterceptor<T> + 'static,
// 	{
// 		self.table_pre_delete.push(Arc::new(interceptor));
// 		self
// 	}
// 
// 	pub fn add_table_post_delete<I>(mut self, interceptor: I) -> Self
// 	where
// 		I: TablePostDeleteInterceptor<T> + 'static,
// 	{
// 		self.table_post_delete.push(Arc::new(interceptor));
// 		self
// 	}
// 
// 	pub fn add_pre_commit<I>(mut self, interceptor: I) -> Self
// 	where
// 		I: PreCommitInterceptor<T> + 'static,
// 	{
// 		self.pre_commit.push(Arc::new(interceptor));
// 		self
// 	}
// 
// 	pub fn add_post_commit<I>(mut self, interceptor: I) -> Self
// 	where
// 		I: PostCommitInterceptor<T> + 'static,
// 	{
// 		self.post_commit.push(Arc::new(interceptor));
// 		self
// 	}
// 
// 	// pub fn add_pre_commit_fn<F>(mut self, f: F) -> Self
// 	// where
// 	// 	F: Fn(&mut
// 	// reifydb_core::interceptor::transaction::context::PreCommitContext<T>)
// 	// -> reifydb_core::Result<()>
// 	// 		+ Send
// 	// 		+ Sync
// 	// 		+ 'static,
// 	// {
// 	// 	self.pre_commit.push(Arc::new(ClosurePreCommitInterceptor::new(f)));
// 	// 	self
// 	// }
// 	//
// 	// pub fn add_post_commit<I>(mut self, interceptor: I) -> Self
// 	// where
// 	// 	I: PostCommitInterceptor<T> + 'static,
// 	// {
// 	// 	self.post_commit.push(Arc::new(interceptor));
// 	// 	self
// 	// }
// 	//
// 	// pub fn add_post_commit_fn<F>(mut self, f: F) -> Self
// 	// where
// 	// 	F: Fn(&mut
// 	// reifydb_core::interceptor::transaction::context::PostCommitContext)
// 	// -> reifydb_core::Result<()>
// 	// 		+ Send
// 	// 		+ Sync
// 	// 		+ 'static,
// 	// {
// 	// 	use reifydb_core::interceptor::closure::ClosurePostCommitInterceptor;
// 	// 	self.post_commit.
// 	// push(Arc::new(ClosurePostCommitInterceptor::new(f))); 	self
// 	// }
// 
// 	pub fn build(self) -> StandardInterceptorFactory<T> {
// 		let mut interceptors = Interceptors::new();
// 
// 		for interceptor in self.table_pre_insert {
// 			interceptors.add_table_pre_insert(interceptor);
// 		}
// 		for interceptor in self.table_post_insert {
// 			interceptors.add_table_post_insert(interceptor);
// 		}
// 		for interceptor in self.table_pre_update {
// 			interceptors.add_table_pre_update(interceptor);
// 		}
// 		for interceptor in self.table_post_update {
// 			interceptors.add_table_post_update(interceptor);
// 		}
// 		for interceptor in self.table_pre_delete {
// 			interceptors.add_table_pre_delete(interceptor);
// 		}
// 		for interceptor in self.table_post_delete {
// 			interceptors.add_table_post_delete(interceptor);
// 		}
// 		for interceptor in self.pre_commit {
// 			interceptors.add_pre_commit(interceptor);
// 		}
// 		for interceptor in self.post_commit {
// 			interceptors.add_post_commit(interceptor);
// 		}
// 
// 		StandardInterceptorFactory {
// 			interceptors,
// 		}
// 	}
// }
// 
// /// Standard implementation of InterceptorFactory
// pub struct StandardInterceptorFactory<T: Transaction> {
// 	interceptors: Interceptors<T>,
// }
// 
// impl<T: Transaction> Default for StandardInterceptorFactory<T> {
// 	fn default() -> Self {
// 		Self {
// 			interceptors: Interceptors::new(),
// 		}
// 	}
// }
// 
// impl<T: Transaction> InterceptorFactory<T> for StandardInterceptorFactory<T> {
// 	fn create(&self) -> Interceptors<T> {
// 		self.interceptors.clone()
// 	}
// }
