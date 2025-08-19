// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use crate::{
	interceptor::{
		PostCommitInterceptor, PreCommitInterceptor,
		TablePostDeleteInterceptor, TablePostInsertInterceptor,
		TablePostUpdateInterceptor, TablePreDeleteInterceptor,
		TablePreInsertInterceptor, TablePreUpdateInterceptor,
		factory::StandardInterceptorFactory,
	},
	interface::Transaction,
};

pub trait AddToBuilder<T: Transaction> {
	fn add_to_builder(
		self,
		builder: StandardInterceptorBuilder<T>,
	) -> StandardInterceptorBuilder<T>;
}

/// Macro to generate add methods for interceptor builder
macro_rules! define_builder_methods {
	(
		$(
			$field:ident: $trait_type:ident => $method_name:ident
		),* $(,)?
	) => {
		$(
			pub fn $method_name<I>(mut self, interceptor: I) -> Self
			where
				I: $trait_type<T> + 'static,
			{
				self.$field.push(Arc::new(interceptor));
				self
			}
		)*
	};
}

/// Builder for configuring interceptors
pub struct StandardInterceptorBuilder<T: Transaction> {
	table_pre_insert: Vec<Arc<dyn TablePreInsertInterceptor<T>>>,
	table_post_insert: Vec<Arc<dyn TablePostInsertInterceptor<T>>>,
	table_pre_update: Vec<Arc<dyn TablePreUpdateInterceptor<T>>>,
	table_post_update: Vec<Arc<dyn TablePostUpdateInterceptor<T>>>,
	table_pre_delete: Vec<Arc<dyn TablePreDeleteInterceptor<T>>>,
	table_post_delete: Vec<Arc<dyn TablePostDeleteInterceptor<T>>>,
	pre_commit: Vec<Arc<dyn PreCommitInterceptor<T>>>,
	post_commit: Vec<Arc<dyn PostCommitInterceptor<T>>>,
}

impl<T: Transaction> Default for StandardInterceptorBuilder<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> StandardInterceptorBuilder<T> {
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

	/// Add any interceptor - the type determines which chain it goes to
	pub fn add_interceptor<I>(self, interceptor: I) -> Self
	where
		I: AddToBuilder<T>,
	{
		interceptor.add_to_builder(self)
	}

	// Generate all the add methods using the macro
	define_builder_methods! {
		table_pre_insert: TablePreInsertInterceptor => add_table_pre_insert,
		table_post_insert: TablePostInsertInterceptor => add_table_post_insert,
		table_pre_update: TablePreUpdateInterceptor => add_table_pre_update,
		table_post_update: TablePostUpdateInterceptor => add_table_post_update,
		table_pre_delete: TablePreDeleteInterceptor => add_table_pre_delete,
		table_post_delete: TablePostDeleteInterceptor => add_table_post_delete,
		pre_commit: PreCommitInterceptor => add_pre_commit,
		post_commit: PostCommitInterceptor => add_post_commit,
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
