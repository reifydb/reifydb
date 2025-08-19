// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	any::{Any, type_name},
	sync::{Arc, OnceLock},
};

use reifydb_core::{Result, diagnostic::internal, error};

use super::IocContainer;

/// Inner storage for lazy resolution
struct LazyResolveInner<T> {
	value: OnceLock<T>,
}

/// Thread-safe lazy resolution wrapper using OnceLock
/// Can be cheaply cloned as it uses Arc internally
pub struct LazyResolve<T> {
	inner: Arc<LazyResolveInner<T>>,
}

#[allow(dead_code)]
impl<T: Clone> LazyResolve<T> {
	/// Create a new lazy resolver
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
	pub fn get_or_resolve(&self, ioc: &IocContainer) -> Result<&T>
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

impl<T: Clone> Clone for LazyResolve<T> {
	fn clone(&self) -> Self {
		Self {
			inner: Arc::clone(&self.inner),
		}
	}
}

impl<T: Clone> Default for LazyResolve<T> {
	fn default() -> Self {
		Self::new()
	}
}

/// Helper function to create a resolver closure for a specific type
#[allow(dead_code)]
pub fn resolver<T>(ioc: &IocContainer) -> impl FnOnce() -> Result<T> + '_
where
	T: Clone + Any + Send + Sync + 'static,
{
	move || ioc.resolve::<T>()
}
