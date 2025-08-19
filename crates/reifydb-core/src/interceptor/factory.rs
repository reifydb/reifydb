// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{interceptor::Interceptors, interface::Transaction};

/// Factory trait for creating interceptor instances for each transaction
pub trait InterceptorFactory<T: Transaction>: Send + Sync {
	/// Create a new instance of interceptors for a transaction
	fn create(&self) -> Interceptors<T>;
}

/// Standard implementation of InterceptorFactory that stores factory functions
/// This allows the factory to be Send+Sync while creating non-Send/Sync
/// interceptors
pub struct StandardInterceptorFactory<T: Transaction> {
	pub(crate) factories:
		Vec<Box<dyn Fn(&mut Interceptors<T>) + Send + Sync>>,
}

impl<T: Transaction> Default for StandardInterceptorFactory<T> {
	fn default() -> Self {
		Self {
			factories: Vec::new(),
		}
	}
}

impl<T: Transaction> StandardInterceptorFactory<T> {
	/// Add a custom factory that directly registers interceptors
	pub fn add(
		&mut self,
		factory: Box<dyn Fn(&mut Interceptors<T>) + Send + Sync>,
	) {
		self.factories.push(factory);
	}
}

impl<T: Transaction> InterceptorFactory<T> for StandardInterceptorFactory<T> {
	fn create(&self) -> Interceptors<T> {
		let mut interceptors = Interceptors::new();

		for factory in &self.factories {
			factory(&mut interceptors);
		}

		interceptors
	}
}
