// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::{Any, TypeId, type_name},
	collections::HashMap,
	sync::Arc,
};

use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::Result;

use crate::internal_error;

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
		self.dependencies.write().insert(TypeId::of::<T>(), BoxedValue::new(service));
		self
	}

	pub fn register_service<T: Clone + Any + Send + Sync + 'static>(&self, service: T) {
		self.dependencies.write().insert(TypeId::of::<T>(), BoxedValue::new(service));
	}

	pub fn clear(&self) {
		self.dependencies.write().clear();
	}

	pub fn resolve<T: Clone + Any + Send + Sync + 'static>(&self) -> Result<T> {
		self.dependencies
			.read()
			.get(&TypeId::of::<T>())
			.and_then(|boxed| boxed.value::<T>())
			.ok_or_else(|| internal_error!("Type {} not registered in IoC container", type_name::<T>()))
	}

	pub fn try_resolve<T: Clone + Any + Send + Sync + 'static>(&self) -> Option<T> {
		self.dependencies.read().get(&TypeId::of::<T>()).and_then(|boxed| boxed.value::<T>())
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
