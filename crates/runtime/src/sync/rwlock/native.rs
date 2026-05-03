// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::{Deref, DerefMut};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct RwLockInner<T> {
	inner: RwLock<T>,
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
