// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interceptor::Interceptors;

/// Factory trait for creating interceptor instances for each MultiVersionCommandTransaction
pub trait InterceptorFactory: Send + Sync {
	/// Create a new instance of interceptors for a MultiVersionCommandTransaction
	fn create(&self) -> Interceptors;
}

/// Standard implementation of InterceptorFactory that stores factory functions
/// This allows the factory to be Send+Sync while creating non-Send/Sync
/// interceptors
pub struct StandardInterceptorFactory {
	pub(crate) factories: Vec<Box<dyn Fn(&mut Interceptors) + Send + Sync>>,
}

impl Default for StandardInterceptorFactory {
	fn default() -> Self {
		Self {
			factories: Vec::new(),
		}
	}
}

impl StandardInterceptorFactory {
	/// Add a custom factory that directly registers interceptors
	pub fn add(&mut self, factory: Box<dyn Fn(&mut Interceptors) + Send + Sync>) {
		self.factories.push(factory);
	}
}

impl InterceptorFactory for StandardInterceptorFactory {
	fn create(&self) -> Interceptors {
		let mut interceptors = Interceptors::new();

		for factory in &self.factories {
			factory(&mut interceptors);
		}

		interceptors
	}
}
