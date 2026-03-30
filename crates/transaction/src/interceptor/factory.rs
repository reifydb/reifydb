// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, RwLock};

use crate::interceptor::interceptors::Interceptors;

type InterceptorFactoryFn = Box<dyn Fn(&mut Interceptors) + Send + Sync>;
type LateInterceptorFactoryFn = Arc<dyn Fn(&mut Interceptors) + Send + Sync>;

/// Concrete factory for creating interceptor instances for each transaction.
///
/// Stores both eagerly-registered factory closures (via `add`) and
/// late-bound factory closures (via `add_late`) that can be registered
/// after construction through `&self`.
pub struct InterceptorFactory {
	pub(crate) factories: Vec<InterceptorFactoryFn>,
	late: RwLock<Vec<LateInterceptorFactoryFn>>,
}

impl Default for InterceptorFactory {
	fn default() -> Self {
		Self {
			factories: Vec::new(),
			late: RwLock::new(Vec::new()),
		}
	}
}

impl InterceptorFactory {
	/// Add a factory closure during construction (requires `&mut self`).
	pub fn add(&mut self, factory: InterceptorFactoryFn) {
		self.factories.push(factory);
	}

	/// Add a late-bound factory closure (thread-safe, takes `&self`).
	pub fn add_late(&self, factory: LateInterceptorFactoryFn) {
		self.late.write().unwrap().push(factory);
	}

	pub fn clear_late(&self) {
		self.late.write().unwrap().clear();
	}

	/// Create a new set of interceptors by invoking all registered factories.
	pub fn create(&self) -> Interceptors {
		let mut interceptors = Interceptors::new();

		for factory in &self.factories {
			factory(&mut interceptors);
		}

		for factory in self.late.read().unwrap().iter() {
			factory(&mut interceptors);
		}

		interceptors
	}
}
