// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{interceptor::Interceptors, interface::CommandTransaction};

/// Factory trait for creating interceptor instances for each CommandTransaction
pub trait InterceptorFactory<CT: CommandTransaction>: Send + Sync {
	/// Create a new instance of interceptors for a CommandTransaction
	fn create(&self) -> Interceptors<CT>;
}

/// Standard implementation of InterceptorFactory that stores factory functions
/// This allows the factory to be Send+Sync while creating non-Send/Sync
/// interceptors
pub struct StandardInterceptorFactory<CT: CommandTransaction> {
	pub(crate) factories:
		Vec<Box<dyn Fn(&mut Interceptors<CT>) + Send + Sync>>,
}

impl<CT: CommandTransaction> Default for StandardInterceptorFactory<CT> {
	fn default() -> Self {
		Self {
			factories: Vec::new(),
		}
	}
}

impl<CT: CommandTransaction> StandardInterceptorFactory<CT> {
	/// Add a custom factory that directly registers interceptors
	pub fn add(
		&mut self,
		factory: Box<dyn Fn(&mut Interceptors<CT>) + Send + Sync>,
	) {
		self.factories.push(factory);
	}
}

impl<CT: CommandTransaction> InterceptorFactory<CT>
	for StandardInterceptorFactory<CT>
{
	fn create(&self) -> Interceptors<CT> {
		let mut interceptors = Interceptors::new();

		for factory in &self.factories {
			factory(&mut interceptors);
		}

		interceptors
	}
}
