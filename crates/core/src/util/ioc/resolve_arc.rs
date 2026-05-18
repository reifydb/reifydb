// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::{Any, type_name},
	sync::{Arc, OnceLock},
};

use reifydb_type::Result;

use super::IocContainer;
use crate::internal_error;

pub struct LazyResolveArc<T> {
	inner: Arc<LazyResolveInner<T>>,
}

struct LazyResolveInner<T> {
	value: OnceLock<T>,
}

#[allow(dead_code)]
impl<T: Clone> LazyResolveArc<T> {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(LazyResolveInner {
				value: OnceLock::new(),
			}),
		}
	}

	pub fn get_or_resolve(&self, ioc: &IocContainer) -> Result<&T>
	where
		T: Clone + Any + Send + Sync + 'static,
	{
		if let Some(value) = self.inner.value.get() {
			return Ok(value);
		}

		let resolved = ioc.resolve::<T>()?;
		match self.inner.value.set(resolved) {
			Ok(()) => self.inner.value.get().ok_or_else(|| {
				internal_error!(
					"Failed to get value after setting in OnceLock for type {}",
					type_name::<T>()
				)
			}),
			Err(_) => self.inner.value.get().ok_or_else(|| {
				internal_error!("Failed to get value from OnceLock for type {}", type_name::<T>())
			}),
		}
	}

	#[allow(dead_code)]
	pub fn get(&self) -> Option<&T> {
		self.inner.value.get()
	}

	#[allow(dead_code)]
	pub fn is_resolved(&self) -> bool {
		self.inner.value.get().is_some()
	}
}

impl<T: Clone> Clone for LazyResolveArc<T> {
	fn clone(&self) -> Self {
		Self {
			inner: Arc::clone(&self.inner),
		}
	}
}

impl<T: Clone> Default for LazyResolveArc<T> {
	fn default() -> Self {
		Self::new()
	}
}

#[allow(dead_code)]
pub fn resolver<T>(ioc: &IocContainer) -> impl FnOnce() -> Result<T> + '_
where
	T: Clone + Any + Send + Sync + 'static,
{
	move || ioc.resolve::<T>()
}
