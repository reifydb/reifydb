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

/// Trait for types that can be added to the interceptor builder
/// This allows both direct interceptors and factory functions
pub trait AddToBuilder<T: Transaction> {
	fn add_to_builder(
		self,
		builder: StandardInterceptorBuilder<T>,
	) -> StandardInterceptorBuilder<T>;
}

/// Macro to generate builder methods for adding interceptors
macro_rules! impl_builder_method {
	($method_name:ident, $trait_type:ty, $factory_method:ident) => {
		pub fn $method_name<F>(mut self, factory_fn: F) -> Self
		where
			F: Fn() -> Arc<$trait_type> + Send + Sync + 'static,
		{
			self.factory.$factory_method(factory_fn);
			self
		}
	};
}

/// Builder for configuring interceptors using factory functions
/// This allows building a Send+Sync factory that creates non-Send/Sync
/// interceptors
pub struct StandardInterceptorBuilder<T: Transaction> {
	factory: StandardInterceptorFactory<T>,
}

impl<T: Transaction> Default for StandardInterceptorBuilder<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: Transaction> StandardInterceptorBuilder<T> {
	pub fn new() -> Self {
		Self {
			factory: StandardInterceptorFactory::default(),
		}
	}

	/// Add an interceptor using the AddToBuilder trait
	/// This maintains backward compatibility with the existing API
	pub fn add_interceptor<I>(self, interceptor: I) -> Self
	where
		I: AddToBuilder<T>,
	{
		interceptor.add_to_builder(self)
	}

	impl_builder_method!(
		add_table_pre_insert,
		dyn TablePreInsertInterceptor<T>,
		add_table_pre_insert_factory
	);
	impl_builder_method!(
		add_table_post_insert,
		dyn TablePostInsertInterceptor<T>,
		add_table_post_insert_factory
	);
	impl_builder_method!(
		add_table_pre_update,
		dyn TablePreUpdateInterceptor<T>,
		add_table_pre_update_factory
	);
	impl_builder_method!(
		add_table_post_update,
		dyn TablePostUpdateInterceptor<T>,
		add_table_post_update_factory
	);
	impl_builder_method!(
		add_table_pre_delete,
		dyn TablePreDeleteInterceptor<T>,
		add_table_pre_delete_factory
	);
	impl_builder_method!(
		add_table_post_delete,
		dyn TablePostDeleteInterceptor<T>,
		add_table_post_delete_factory
	);
	impl_builder_method!(
		add_pre_commit,
		dyn PreCommitInterceptor<T>,
		add_pre_commit_factory
	);
	impl_builder_method!(
		add_post_commit,
		dyn PostCommitInterceptor<T>,
		add_post_commit_factory
	);

	pub fn build(self) -> StandardInterceptorFactory<T> {
		self.factory
	}
}
