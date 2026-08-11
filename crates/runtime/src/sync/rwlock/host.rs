// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::disallowed_types)]

use std::{
	fmt,
	ops::{Deref, DerefMut},
	sync::Arc,
};

use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct RwLockInner<T> {
	inner: RwLock<T>,
}

impl<T: fmt::Debug> fmt::Debug for RwLockInner<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.inner.fmt(f)
	}
}

impl<T> RwLockInner<T> {
	pub fn new(value: T) -> Self {
		Self {
			inner: RwLock::new(value),
		}
	}

	pub fn read(&self) -> RwLockReadGuardInner<'_, T> {
		RwLockReadGuardInner {
			inner: self.inner.read(),
		}
	}

	pub fn write(&self) -> RwLockWriteGuardInner<'_, T> {
		RwLockWriteGuardInner {
			inner: self.inner.write(),
		}
	}

	pub fn try_read(&self) -> Option<RwLockReadGuardInner<'_, T>> {
		self.inner.try_read().map(|guard| RwLockReadGuardInner {
			inner: guard,
		})
	}

	pub fn try_write(&self) -> Option<RwLockWriteGuardInner<'_, T>> {
		self.inner.try_write().map(|guard| RwLockWriteGuardInner {
			inner: guard,
		})
	}

	pub fn read_recursive(&self) -> RwLockReadGuardInner<'_, T> {
		RwLockReadGuardInner {
			inner: self.inner.read_recursive(),
		}
	}

	pub fn try_read_recursive(&self) -> Option<RwLockReadGuardInner<'_, T>> {
		self.inner.try_read_recursive().map(|guard| RwLockReadGuardInner {
			inner: guard,
		})
	}
}

pub struct RwLockReadGuardInner<'a, T> {
	inner: RwLockReadGuard<'a, T>,
}

impl<'a, T> Deref for RwLockReadGuardInner<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

pub struct RwLockWriteGuardInner<'a, T> {
	inner: RwLockWriteGuard<'a, T>,
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

pub struct ArcRwLockInner<T> {
	inner: Arc<RwLock<T>>,
}

impl<T> Clone for ArcRwLockInner<T> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl<T> ArcRwLockInner<T> {
	pub fn new(value: T) -> Self {
		Self {
			inner: Arc::new(RwLock::new(value)),
		}
	}

	pub fn read(&self) -> OwnedRwLockReadGuardInner<T> {
		OwnedRwLockReadGuardInner {
			inner: self.inner.read_arc(),
		}
	}

	pub fn write(&self) -> OwnedRwLockWriteGuardInner<T> {
		OwnedRwLockWriteGuardInner {
			inner: self.inner.write_arc(),
		}
	}
}

pub struct OwnedRwLockReadGuardInner<T> {
	inner: ArcRwLockReadGuard<RawRwLock, T>,
}

impl<T> Deref for OwnedRwLockReadGuardInner<T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

pub struct OwnedRwLockWriteGuardInner<T> {
	inner: ArcRwLockWriteGuard<RawRwLock, T>,
}

impl<T> Deref for OwnedRwLockWriteGuardInner<T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

impl<T> DerefMut for OwnedRwLockWriteGuardInner<T> {
	fn deref_mut(&mut self) -> &mut T {
		&mut self.inner
	}
}
