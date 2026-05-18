// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cell,
	cell::RefMut,
	fmt,
	ops::{Deref, DerefMut},
};

pub struct MutexInner<T> {
	inner: cell::RefCell<T>,
}

impl<T: fmt::Debug> fmt::Debug for MutexInner<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("MutexInner").field("data", &self.inner).finish()
	}
}

impl<T> MutexInner<T> {
	pub fn new(value: T) -> Self {
		Self {
			inner: cell::RefCell::new(value),
		}
	}

	pub fn lock(&self) -> MutexGuardInner<'_, T> {
		MutexGuardInner {
			inner: self.inner.borrow_mut(),
		}
	}

	pub fn try_lock(&self) -> Option<MutexGuardInner<'_, T>> {
		self.inner.try_borrow_mut().ok().map(|inner| MutexGuardInner {
			inner,
		})
	}
}

pub struct MutexGuardInner<'a, T> {
	pub(in crate::sync) inner: RefMut<'a, T>,
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
