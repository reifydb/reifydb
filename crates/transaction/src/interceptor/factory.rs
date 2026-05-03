// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, RwLock};

use crate::interceptor::interceptors::Interceptors;

type InterceptorFactoryFn = Box<dyn Fn(&mut Interceptors) + Send + Sync>;
type LateInterceptorFactoryFn = Arc<dyn Fn(&mut Interceptors) + Send + Sync>;

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
	pub fn add(&mut self, factory: InterceptorFactoryFn) {
		self.factories.push(factory);
	}

	pub fn add_late(&self, factory: LateInterceptorFactoryFn) {
		self.late.write().unwrap().push(factory);
	}

	pub fn clear_late(&self) {
		self.late.write().unwrap().clear();
	}

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
