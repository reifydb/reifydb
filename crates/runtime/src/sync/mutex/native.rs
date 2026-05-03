// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt,
	ops::{Deref, DerefMut},
};

use parking_lot::{Mutex, MutexGuard};

pub struct MutexInner<T> {
	inner: Mutex<T>,
}

impl<T: fmt::Debug> fmt::Debug for MutexInner<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.inner.fmt(f)
	}
}

impl<T> MutexInner<T> {
	pub fn new(value: T) -> Self {
		Self {
			inner: Mutex::new(value),
		}
	}

	pub fn lock(&self) -> MutexGuardInner<'_, T> {
		MutexGuardInner {
			inner: self.inner.lock(),
		}
	}

	pub fn try_lock(&self) -> Option<MutexGuardInner<'_, T>> {
		self.inner.try_lock().map(|guard| MutexGuardInner {
			inner: guard,
		})
	}
}

pub struct MutexGuardInner<'a, T> {
	pub(in crate::sync) inner: MutexGuard<'a, T>,
}

impl<'a, T> Deref for MutexGuardInner<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

impl<'a, T> DerefMut for MutexGuardInner<'a, T> {
	fn deref_mut(&mut self) -> &mut T {
		&mut self.inner
	}
}
