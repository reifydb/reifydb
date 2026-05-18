// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::{Any, type_name},
	cell::OnceCell,
	rc::Rc,
};

use reifydb_type::Result;

use super::IocContainer;
use crate::internal_error;

pub struct LazyResolveRc<T> {
	inner: Rc<LazyResolveInner<T>>,
}

struct LazyResolveInner<T> {
	value: OnceCell<T>,
}

#[allow(dead_code)]
impl<T: Clone> LazyResolveRc<T> {
	pub fn new() -> Self {
		Self {
			inner: Rc::new(LazyResolveInner {
				value: OnceCell::new(),
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
					"Failed to get value after setting in OnceCell for type {}",
					type_name::<T>()
				)
			}),
			Err(_) => Err(internal_error!("Failed to set value in OnceCell for type {}", type_name::<T>())),
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

impl<T: Clone> Clone for LazyResolveRc<T> {
	fn clone(&self) -> Self {
		Self {
			inner: Rc::clone(&self.inner),
		}
	}
}

impl<T: Clone> Default for LazyResolveRc<T> {
	fn default() -> Self {
		Self::new()
	}
}
