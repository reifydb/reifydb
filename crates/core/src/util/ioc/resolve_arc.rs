// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	any::{Any, type_name},
	sync::{Arc, OnceLock},
};

use reifydb_type::error;

use super::IocContainer;
use crate::error::diagnostic::internal::internal;

/// Thread-safe lazy resolution wrapper using OnceLock
/// Can be cheaply cloned as it uses Arc internally
pub struct LazyResolveArc<T> {
	inner: Arc<LazyResolveInner<T>>,
}

/// Inner storage for lazy resolution
struct LazyResolveInner<T> {
	value: OnceLock<T>,
}

#[allow(dead_code)]
impl<T: Clone> LazyResolveArc<T> {
	/// Create a new lazy resolve
	pub fn new() -> Self {
		Self {
			inner: Arc::new(LazyResolveInner {
				value: OnceLock::new(),
			}),
		}
	}

	/// Get or resolve the value from the IoC container
	/// The resolution happens exactly once, subsequent calls return the
	/// cached value
	pub fn get_or_resolve(&self, ioc: &IocContainer) -> reifydb_type::Result<&T>
	where
		T: Clone + Any + Send + Sync + 'static,
	{
		if let Some(value) = self.inner.value.get() {
			return Ok(value);
		}

		// Try to resolve and set
		let resolved = ioc.resolve::<T>()?;
		match self.inner.value.set(resolved) {
			Ok(()) => {
				// We successfully set it, return a reference
				self.inner.value.get().ok_or_else(|| {
					error!(internal(format!(
						"Failed to get value after setting in OnceLock for type {}",
						type_name::<T>()
					)))
				})
			}
			Err(_) => {
				// Someone else set it in the meantime, use
				// their value
				self.inner.value.get().ok_or_else(|| {
					error!(internal(format!(
						"Failed to get value from OnceLock for type {}",
						type_name::<T>()
					)))
				})
			}
		}
	}

	/// Get the resolved value if it exists, without attempting to resolve
	#[allow(dead_code)]
	pub fn get(&self) -> Option<&T> {
		self.inner.value.get()
	}

	/// Check if the value has been resolved
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

/// Helper function to create a resolve closure for a specific type
#[allow(dead_code)]
pub fn resolver<T>(ioc: &IocContainer) -> impl FnOnce() -> reifydb_type::Result<T> + '_
where
	T: Clone + Any + Send + Sync + 'static,
{
	move || ioc.resolve::<T>()
}
