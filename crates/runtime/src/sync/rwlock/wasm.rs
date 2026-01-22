// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM RwLock implementation (no-op).
//!
//! Since WASM is single-threaded, this is a simple wrapper around RefCell.

use std::{
	cell::{Ref, RefMut},
	ops::{Deref, DerefMut},
};

/// WASM reader-writer lock implementation using RefCell (no actual locking needed).
pub struct RwLockInner<T> {
	inner: std::cell::RefCell<T>,
}

impl<T> RwLockInner<T> {
	/// Creates a new reader-writer lock.
	pub fn new(value: T) -> Self {
		Self {
			inner: std::cell::RefCell::new(value),
		}
	}

	/// Acquires a read lock (immutable borrow).
	pub fn read(&self) -> RwLockReadGuardInner<'_, T> {
		RwLockReadGuardInner {
			inner: self.inner.borrow(),
		}
	}

	/// Acquires a write lock (mutable borrow).
	pub fn write(&self) -> RwLockWriteGuardInner<'_, T> {
		RwLockWriteGuardInner {
			inner: self.inner.borrow_mut(),
		}
	}

	/// Attempts to acquire a read lock.
	pub fn try_read(&self) -> Option<RwLockReadGuardInner<'_, T>> {
		self.inner.try_borrow().ok().map(|inner| RwLockReadGuardInner {
			inner,
		})
	}

	/// Attempts to acquire a write lock.
	pub fn try_write(&self) -> Option<RwLockWriteGuardInner<'_, T>> {
		self.inner.try_borrow_mut().ok().map(|inner| RwLockWriteGuardInner {
			inner,
		})
	}
}

/// WASM guard providing read access to the data protected by an RwLock.
pub struct RwLockReadGuardInner<'a, T> {
	inner: Ref<'a, T>,
}

impl<'a, T> Deref for RwLockReadGuardInner<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

/// WASM guard providing write access to the data protected by an RwLock.
pub struct RwLockWriteGuardInner<'a, T> {
	inner: RefMut<'a, T>,
}

impl<'a, T> Deref for RwLockWriteGuardInner<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

impl<'a, T> DerefMut for RwLockWriteGuardInner<'a, T> {
	fn deref_mut(&mut self) -> &mut T {
		&mut self.inner
	}
}
