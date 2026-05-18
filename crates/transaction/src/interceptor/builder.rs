// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{factory::InterceptorFactory, interceptors::Interceptors};

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
