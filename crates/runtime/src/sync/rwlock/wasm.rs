// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cell,
	cell::{Ref, RefMut},
	ops::{Deref, DerefMut},
};

pub struct RwLockInner<T> {
	inner: cell::RefCell<T>,
}

impl<T> RwLockInner<T> {
	pub fn new(value: T) -> Self {
		Self {
			inner: cell::RefCell::new(value),
		}
	}

	pub fn read(&self) -> RwLockReadGuardInner<'_, T> {
		RwLockReadGuardInner {
			inner: self.inner.borrow(),
		}
	}

	pub fn write(&self) -> RwLockWriteGuardInner<'_, T> {
		RwLockWriteGuardInner {
			inner: self.inner.borrow_mut(),
		}
	}

	pub fn try_read(&self) -> Option<RwLockReadGuardInner<'_, T>> {
		self.inner.try_borrow().ok().map(|inner| RwLockReadGuardInner {
			inner,
		})
	}

	pub fn try_write(&self) -> Option<RwLockWriteGuardInner<'_, T>> {
		self.inner.try_borrow_mut().ok().map(|inner| RwLockWriteGuardInner {
			inner,
		})
	}
}

pub struct RwLockReadGuardInner<'a, T> {
	inner: Ref<'a, T>,
}

impl<'a, T> Deref for RwLockReadGuardInner<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

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
