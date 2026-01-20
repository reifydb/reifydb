// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native RwLock implementation using parking_lot.

use std::ops::{Deref, DerefMut};

/// A reader-writer lock for shared read access and exclusive write access.
///
/// Native implementation wraps parking_lot::RwLock.
pub struct RwLock<T> {
	inner: parking_lot::RwLock<T>,
}

impl<T> RwLock<T> {
	/// Creates a new reader-writer lock.
	pub fn new(value: T) -> Self {
		Self {
			inner: parking_lot::RwLock::new(value),
		}
	}

	/// Acquires a read lock, blocking until it's available.
	pub fn read(&self) -> RwLockReadGuard<'_, T> {
		RwLockReadGuard {
			inner: self.inner.read(),
		}
	}

	/// Acquires a write lock, blocking until it's available.
	pub fn write(&self) -> RwLockWriteGuard<'_, T> {
		RwLockWriteGuard {
			inner: self.inner.write(),
		}
	}

	/// Attempts to acquire a read lock without blocking.
	pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
		self.inner.try_read().map(|guard| RwLockReadGuard { inner: guard })
	}

	/// Attempts to acquire a write lock without blocking.
	pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
		self.inner.try_write().map(|guard| RwLockWriteGuard { inner: guard })
	}
}

/// A guard providing read access to the data protected by an RwLock.
pub struct RwLockReadGuard<'a, T> {
	inner: parking_lot::RwLockReadGuard<'a, T>,
}

impl<'a, T> Deref for RwLockReadGuard<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

/// A guard providing write access to the data protected by an RwLock.
pub struct RwLockWriteGuard<'a, T> {
	inner: parking_lot::RwLockWriteGuard<'a, T>,
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
