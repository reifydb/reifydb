// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	ops::{Deref, DerefMut},
};

use loom::sync::{Mutex, MutexGuard};

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
			inner: Some(self.inner.lock().expect("loom mutex poisoned")),
		}
	}

	pub fn try_lock(&self) -> Option<MutexGuardInner<'_, T>> {
		self.inner.try_lock().ok().map(|guard| MutexGuardInner {
			inner: Some(guard),
		})
	}
}

pub struct MutexGuardInner<'a, T> {
	pub(in crate::sync) inner: Option<MutexGuard<'a, T>>,
}

impl<'a, T> MutexGuardInner<'a, T> {
	pub(in crate::sync) fn take(&mut self) -> MutexGuard<'a, T> {
		self.inner.take().expect("loom guard already surrendered to a condvar wait")
	}

	pub(in crate::sync) fn restore(&mut self, guard: MutexGuard<'a, T>) {
		self.inner = Some(guard);
	}
}

impl<'a, T> Deref for MutexGuardInner<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		self.inner.as_ref().expect("loom guard already surrendered to a condvar wait")
	}
}

impl<'a, T> DerefMut for MutexGuardInner<'a, T> {
	fn deref_mut(&mut self) -> &mut T {
		self.inner.as_mut().expect("loom guard already surrendered to a condvar wait")
	}
}
