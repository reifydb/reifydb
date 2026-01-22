// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native RwLock implementation using parking_lot.

use std::ops::{Deref, DerefMut};

/// Native reader-writer lock implementation wrapping parking_lot::RwLock.
pub struct RwLockInner<T> {
	inner: parking_lot::RwLock<T>,
}

impl<T> RwLockInner<T> {
	/// Creates a new reader-writer lock.
	pub fn new(value: T) -> Self {
		Self {
			inner: parking_lot::RwLock::new(value),
		}
	}

	/// Acquires a read lock, blocking until it's available.
	pub fn read(&self) -> RwLockReadGuardInner<'_, T> {
		RwLockReadGuardInner {
			inner: self.inner.read(),
		}
	}

	/// Acquires a write lock, blocking until it's available.
	pub fn write(&self) -> RwLockWriteGuardInner<'_, T> {
		RwLockWriteGuardInner {
			inner: self.inner.write(),
		}
	}

	/// Attempts to acquire a read lock without blocking.
	pub fn try_read(&self) -> Option<RwLockReadGuardInner<'_, T>> {
		self.inner.try_read().map(|guard| RwLockReadGuardInner { inner: guard })
	}

	/// Attempts to acquire a write lock without blocking.
	pub fn try_write(&self) -> Option<RwLockWriteGuardInner<'_, T>> {
		self.inner.try_write().map(|guard| RwLockWriteGuardInner { inner: guard })
	}
}

/// Native guard providing read access to the data protected by an RwLock.
pub struct RwLockReadGuardInner<'a, T> {
	inner: parking_lot::RwLockReadGuard<'a, T>,
}

impl<'a, T> Deref for RwLockReadGuardInner<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

/// Native guard providing write access to the data protected by an RwLock.
pub struct RwLockWriteGuardInner<'a, T> {
	inner: parking_lot::RwLockWriteGuard<'a, T>,
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
