// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	interceptor::{Interceptors, factory::StandardInterceptorFactory},
	interface::Transaction,
};

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
	pub fn add_factory<F>(mut self, factory: F) -> Self
	where
		F: Fn(&mut Interceptors<T>) + Send + Sync + 'static,
	{
		self.factory.add(Box::new(factory));
		self
	}

	pub fn build(self) -> StandardInterceptorFactory<T> {
		self.factory
	}
}
