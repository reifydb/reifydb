// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{Interceptors, StandardInterceptorFactory};

/// Builder for configuring interceptors using factory functions
/// This allows building a Send+Sync factory that creates non-Send/Sync
/// interceptors
pub struct StandardInterceptorBuilder {
	factory: StandardInterceptorFactory,
}

impl Default for StandardInterceptorBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl StandardInterceptorBuilder {
	pub fn new() -> Self {
		Self {
			factory: StandardInterceptorFactory::default(),
		}
	}
	pub fn add_factory<F>(mut self, factory: F) -> Self
	where
		F: Fn(&mut Interceptors) + Send + Sync + 'static,
	{
		self.factory.add(Box::new(factory));
		self
	}

	pub fn build(self) -> StandardInterceptorFactory {
		self.factory
	}
}
