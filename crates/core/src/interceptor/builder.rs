// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	interceptor::{Interceptors, factory::StandardInterceptorFactory},
	interface::CommandTransaction,
};

/// Builder for configuring interceptors using factory functions
/// This allows building a Send+Sync factory that creates non-Send/Sync
/// interceptors
pub struct StandardInterceptorBuilder<CT: CommandTransaction> {
	factory: StandardInterceptorFactory<CT>,
}

impl<CT: CommandTransaction> Default for StandardInterceptorBuilder<CT> {
	fn default() -> Self {
		Self::new()
	}
}

impl<CT: CommandTransaction> StandardInterceptorBuilder<CT> {
	pub fn new() -> Self {
		Self {
			factory: StandardInterceptorFactory::default(),
		}
	}
	pub fn add_factory<F>(mut self, factory: F) -> Self
	where
		F: Fn(&mut Interceptors<CT>) + Send + Sync + 'static,
	{
		self.factory.add(Box::new(factory));
		self
	}

	pub fn build(self) -> StandardInterceptorFactory<CT> {
		self.factory
	}
}
