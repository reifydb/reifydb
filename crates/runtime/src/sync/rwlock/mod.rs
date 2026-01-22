// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! RwLock synchronization primitive.

use std::ops::{Deref, DerefMut};

use cfg_if::cfg_if;

#[cfg(reifydb_target = "native")]
pub(crate) mod native;
#[cfg(reifydb_target = "wasm")]
pub(crate) mod wasm;

cfg_if! {
	if #[cfg(reifydb_target = "native")] {
		type RwLockInnerImpl<T> = native::RwLockInner<T>;
		type RwLockReadGuardInnerImpl<'a, T> = native::RwLockReadGuardInner<'a, T>;
		type RwLockWriteGuardInnerImpl<'a, T> = native::RwLockWriteGuardInner<'a, T>;
	} else {
		type RwLockInnerImpl<T> = wasm::RwLockInner<T>;
		type RwLockReadGuardInnerImpl<'a, T> = wasm::RwLockReadGuardInner<'a, T>;
		type RwLockWriteGuardInnerImpl<'a, T> = wasm::RwLockWriteGuardInner<'a, T>;
	}
}

/// A reader-writer lock for shared read access and exclusive write access.
pub struct RwLock<T> {
	inner: RwLockInnerImpl<T>,
}

// SAFETY: WASM is single-threaded, so Sync is safe
#[cfg(reifydb_target = "wasm")]
unsafe impl<T> Sync for RwLock<T> {}

impl<T> RwLock<T> {
	/// Creates a new reader-writer lock.
	#[inline]
	pub fn new(value: T) -> Self {
		Self {
			inner: RwLockInnerImpl::new(value),
		}
	}

	/// Acquires a read lock, blocking until it's available.
	#[inline]
	pub fn read(&self) -> RwLockReadGuard<'_, T> {
		RwLockReadGuard {
			inner: self.inner.read(),
		}
	}

	/// Acquires a write lock, blocking until it's available.
	#[inline]
	pub fn write(&self) -> RwLockWriteGuard<'_, T> {
		RwLockWriteGuard {
			inner: self.inner.write(),
		}
	}

	/// Attempts to acquire a read lock without blocking.
	#[inline]
	pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
		self.inner.try_read().map(|inner| RwLockReadGuard {
			inner,
		})
	}

	/// Attempts to acquire a write lock without blocking.
	#[inline]
	pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
		self.inner.try_write().map(|inner| RwLockWriteGuard {
			inner,
		})
	}
}

/// A guard providing read access to the data protected by an RwLock.
pub struct RwLockReadGuard<'a, T> {
	inner: RwLockReadGuardInnerImpl<'a, T>,
}

impl<'a, T> Deref for RwLockReadGuard<'a, T> {
	type Target = T;

	#[inline]
	fn deref(&self) -> &T {
		&self.inner
	}
}

/// A guard providing write access to the data protected by an RwLock.
pub struct RwLockWriteGuard<'a, T> {
	inner: RwLockWriteGuardInnerImpl<'a, T>,
}

impl<'a, T> Deref for RwLockWriteGuard<'a, T> {
	type Target = T;

	#[inline]
	fn deref(&self) -> &T {
		&self.inner
	}
}

impl<'a, T> DerefMut for RwLockWriteGuard<'a, T> {
	#[inline]
	fn deref_mut(&mut self) -> &mut T {
		&mut self.inner
	}
}
