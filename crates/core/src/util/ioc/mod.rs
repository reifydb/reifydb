// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Inversion-of-control container used to register and resolve long-lived services by Rust type.
//!
//! `IocContainer` is a thread-safe map from `TypeId` to a type-erased value. Crates register implementations at startup
//! (storage backends, evaluators, the event bus, the catalog) and resolve them by type later, which lets the same
//! wiring code support both the in-process embedded runtime and the multi-tenant server runtime without threading every
//! dependency through the call stack.
//!
//! Invariant: only one value per `TypeId` is registered. Registering twice silently overwrites the previous value;
//! consumers should treat the container as immutable after startup wiring is complete.

pub mod resolve_arc;
pub mod resolve_rc;

use std::{
	any::{Any, TypeId, type_name},
	collections::HashMap,
	sync::{Arc, RwLock},
};

use reifydb_type::Result;

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
		self.dependencies.write().unwrap().insert(TypeId::of::<T>(), BoxedValue::new(service));
		self
	}

	pub fn register_service<T: Clone + Any + Send + Sync + 'static>(&self, service: T) {
		self.dependencies.write().unwrap().insert(TypeId::of::<T>(), BoxedValue::new(service));
	}

	pub fn clear(&self) {
		self.dependencies.write().unwrap().clear();
	}

	pub fn resolve<T: Clone + Any + Send + Sync + 'static>(&self) -> Result<T> {
		self.dependencies
			.read()
			.unwrap()
			.get(&TypeId::of::<T>())
			.and_then(|boxed| boxed.value::<T>())
			.ok_or_else(|| internal_error!("Type {} not registered in IoC container", type_name::<T>()))
	}

	pub fn try_resolve<T: Clone + Any + Send + Sync + 'static>(&self) -> Option<T> {
		self.dependencies.read().unwrap().get(&TypeId::of::<T>()).and_then(|boxed| boxed.value::<T>())
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
