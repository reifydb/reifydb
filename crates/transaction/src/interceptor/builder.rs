// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{factory::InterceptorFactory, interceptors::Interceptors};

/// Builder for configuring interceptors using factory functions
/// This allows building a Send+Sync factory that creates non-Send/Sync
/// interceptors
pub struct InterceptorBuilder {
	factory: InterceptorFactory,
}

impl Default for InterceptorBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl InterceptorBuilder {
	pub fn new() -> Self {
		Self {
			factory: InterceptorFactory::default(),
		}
	}
	pub fn add_factory<F>(mut self, factory: F) -> Self
	where
		F: Fn(&mut Interceptors) + Send + Sync + 'static,
	{
		self.factory.add(Box::new(factory));
		self
	}

	pub fn build(self) -> InterceptorFactory {
		self.factory
	}
}
