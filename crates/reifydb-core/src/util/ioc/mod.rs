// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod resolve;
mod single_thread;

use std::{
	any::{Any, TypeId, type_name},
	collections::HashMap,
	sync::{Arc, RwLock},
};

#[allow(unused_imports)]
pub use resolve::LazyResolve;
pub use single_thread::SingleThreadLazyResolve;

use crate::{Result, diagnostic::internal, error};

struct BoxedValue {
	value: Box<dyn Any + Send + Sync>,
}

impl BoxedValue {
	fn new<T: Clone + Any + Send + Sync + 'static>(value: T) -> Self {
		Self {
			value: Box::new(value),
		}
	}

	fn value<T: Clone + Any + Send + Sync + 'static>(&self) -> Option<T> {
		self.value.downcast_ref::<T>().cloned()
	}
}

/// Lightweight IoC container for dependency injection
pub struct IocContainer {
	dependencies: Arc<RwLock<HashMap<TypeId, BoxedValue>>>,
}

impl IocContainer {
	pub fn new() -> Self {
		Self {
			dependencies: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn register<T: Clone + Any + Send + Sync + 'static>(
		self,
		service: T,
	) -> Self {
		self.dependencies
			.write()
			.unwrap()
			.insert(TypeId::of::<T>(), BoxedValue::new(service));
		self
	}

	pub fn resolve<T: Clone + Any + Send + Sync + 'static>(
		&self,
	) -> Result<T> {
		self.dependencies
			.read()
			.unwrap()
			.get(&TypeId::of::<T>())
			.and_then(|boxed| boxed.value::<T>())
			.ok_or_else(|| {
				error!(internal(format!(
					"Type {} not registered in IoC container",
					type_name::<T>()
				)))
			})
	}
}

impl Clone for IocContainer {
	fn clone(&self) -> Self {
		Self {
			dependencies: self.dependencies.clone(),
		}
	}
}

impl Default for IocContainer {
	fn default() -> Self {
		Self::new()
	}
}
