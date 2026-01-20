// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM RwLock implementation (no-op).
//!
//! Since WASM is single-threaded, this is a simple wrapper around RefCell.

use std::cell::{Ref, RefMut};
use std::ops::{Deref, DerefMut};

/// A reader-writer lock for shared read access and exclusive write access.
///
/// WASM implementation uses RefCell (no actual locking needed).
pub struct RwLock<T> {
	inner: std::cell::RefCell<T>,
}

// SAFETY: WASM is single-threaded, so Sync is safe
unsafe impl<T> Sync for RwLock<T> {}

impl<T> RwLock<T> {
	/// Creates a new reader-writer lock.
	pub fn new(value: T) -> Self {
		Self {
			inner: std::cell::RefCell::new(value),
		}
	}

	/// Acquires a read lock (immutable borrow).
	pub fn read(&self) -> RwLockReadGuard<'_, T> {
		RwLockReadGuard {
			inner: self.inner.borrow(),
		}
	}

	/// Acquires a write lock (mutable borrow).
	pub fn write(&self) -> RwLockWriteGuard<'_, T> {
		RwLockWriteGuard {
			inner: self.inner.borrow_mut(),
		}
	}

	/// Attempts to acquire a read lock.
	pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
		self.inner.try_borrow().ok().map(|inner| RwLockReadGuard { inner })
	}

	/// Attempts to acquire a write lock.
	pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
		self.inner.try_borrow_mut().ok().map(|inner| RwLockWriteGuard { inner })
	}
}

/// A guard providing read access to the data protected by an RwLock.
pub struct RwLockReadGuard<'a, T> {
	inner: Ref<'a, T>,
}

impl<'a, T> Deref for RwLockReadGuard<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

/// A guard providing write access to the data protected by an RwLock.
pub struct RwLockWriteGuard<'a, T> {
	inner: RefMut<'a, T>,
}

impl<'a, T> Deref for RwLockWriteGuard<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

impl<'a, T> DerefMut for RwLockWriteGuard<'a, T> {
	fn deref_mut(&mut self) -> &mut T {
		&mut self.inner
	}
}
