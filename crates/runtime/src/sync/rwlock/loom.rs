// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	ops::{Deref, DerefMut},
};

use loom::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

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
			inner: self.inner.read().expect("loom rwlock poisoned"),
		}
	}

	pub fn write(&self) -> RwLockWriteGuardInner<'_, T> {
		RwLockWriteGuardInner {
			inner: self.inner.write().expect("loom rwlock poisoned"),
		}
	}

	pub fn try_read(&self) -> Option<RwLockReadGuardInner<'_, T>> {
		self.inner.try_read().ok().map(|guard| RwLockReadGuardInner {
			inner: guard,
		})
	}

	pub fn try_write(&self) -> Option<RwLockWriteGuardInner<'_, T>> {
		self.inner.try_write().ok().map(|guard| RwLockWriteGuardInner {
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
