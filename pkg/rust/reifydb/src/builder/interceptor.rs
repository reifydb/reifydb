// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::mem::take;

use reifydb_core::{
	interceptor::{
		PostCommitInterceptor, PreCommitInterceptor,
		TablePostDeleteInterceptor, TablePostInsertInterceptor,
		TablePostUpdateInterceptor, TablePreDeleteInterceptor,
		TablePreInsertInterceptor, TablePreUpdateInterceptor,
	},
	interface::Transaction,
};
use reifydb_engine::interceptor;

pub trait InterceptorBuilder<T: Transaction>: Sized {
	fn builder(&mut self) -> &mut interceptor::InterceptorBuilder<T>;

	fn add_pre_commit_interceptor<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn PreCommitInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		let builder = self.builder();
		*builder = take(builder).add_pre_commit(factory);
		self
	}

	fn add_post_commit_interceptor<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn PostCommitInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		let builder = self.builder();
		*builder = take(builder).add_post_commit(factory);
		self
	}

	fn add_table_pre_insert_interceptor<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePreInsertInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		let builder = self.builder();
		*builder = take(builder).add_table_pre_insert(factory);
		self
	}

	fn add_table_post_insert_interceptor<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePostInsertInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		let builder = self.builder();
		*builder = take(builder).add_table_post_insert(factory);
		self
	}

	fn add_table_pre_update_interceptor<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePreUpdateInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		let builder = self.builder();
		*builder = take(builder).add_table_pre_update(factory);
		self
	}

	fn add_table_post_update_interceptor<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePostUpdateInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		let builder = self.builder();
		*builder = take(builder).add_table_post_update(factory);
		self
	}

	fn add_table_pre_delete_interceptor<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePreDeleteInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		let builder = self.builder();
		*builder = take(builder).add_table_pre_delete(factory);
		self
	}

	fn add_table_post_delete_interceptor<F>(mut self, factory: F) -> Self
	where
		F: Fn() -> Box<dyn TablePostDeleteInterceptor<T>>
			+ Send
			+ Sync
			+ 'static,
	{
		let builder = self.builder();
		*builder = take(builder).add_table_post_delete(factory);
		self
	}
}
