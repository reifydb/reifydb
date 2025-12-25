// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod resolve_arc;
mod resolve_rc;

use std::{
	any::{Any, TypeId, type_name},
	collections::HashMap,
	sync::{Arc, RwLock},
};

use reifydb_type::{Result, diagnostic::internal, error};
#[allow(unused_imports)]
pub use resolve_arc::LazyResolveArc;
pub use resolve_rc::LazyResolveRc;

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

	pub fn register<T: Clone + Any + Send + Sync + 'static>(self, service: T) -> Self {
		self.dependencies.write().unwrap().insert(TypeId::of::<T>(), BoxedValue::new(service));
		self
	}

	/// Register a service from a reference (for late registration after construction)
	pub fn register_service<T: Clone + Any + Send + Sync + 'static>(&self, service: T) {
		self.dependencies.write().unwrap().insert(TypeId::of::<T>(), BoxedValue::new(service));
	}

	pub fn resolve<T: Clone + Any + Send + Sync + 'static>(&self) -> Result<T> {
		self.dependencies
			.read()
			.unwrap()
			.get(&TypeId::of::<T>())
			.and_then(|boxed| boxed.value::<T>())
			.ok_or_else(|| {
				error!(internal(format!("Type {} not registered in IoC container", type_name::<T>())))
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
