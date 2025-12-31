// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	any::{Any, type_name},
	cell::OnceCell,
	rc::Rc,
};

use reifydb_type::{Result, diagnostic::internal, error};

use super::IocContainer;

/// Single-threaded lazy resolution wrapper using OnceCell
/// Can be cheaply cloned as it uses Rc internally
pub struct LazyResolveRc<T> {
	inner: Rc<LazyResolveInner<T>>,
}

/// Inner storage for lazy resolution (single-threaded)
struct LazyResolveInner<T> {
	value: OnceCell<T>,
}

#[allow(dead_code)]
impl<T: Clone> LazyResolveRc<T> {
	/// Create a new lazy resolve
	pub fn new() -> Self {
		Self {
			inner: Rc::new(LazyResolveInner {
				value: OnceCell::new(),
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
						"Failed to get value after setting in OnceCell for type {}",
						type_name::<T>()
					)))
				})
			}
			Err(_) => {
				// This shouldn't happen in single-threaded
				// context
				Err(error!(internal(format!(
					"Failed to set value in OnceCell for type {}",
					type_name::<T>()
				))))
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
