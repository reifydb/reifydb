// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::{
	cell,
	cell::{Ref, RefMut},
	fmt, mem,
	ops::{Deref, DerefMut},
	sync::Arc,
};

pub struct RwLockInner<T> {
	inner: cell::RefCell<T>,
}

impl<T: fmt::Debug> fmt::Debug for RwLockInner<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.inner.fmt(f)
	}
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

	pub fn read_recursive(&self) -> RwLockReadGuardInner<'_, T> {
		RwLockReadGuardInner {
			inner: self.inner.borrow(),
		}
	}

	pub fn try_read_recursive(&self) -> Option<RwLockReadGuardInner<'_, T>> {
		self.inner.try_borrow().ok().map(|inner| RwLockReadGuardInner {
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

pub struct ArcRwLockInner<T> {
	inner: Arc<cell::RefCell<T>>,
}

impl<T> Clone for ArcRwLockInner<T> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl<T: 'static> ArcRwLockInner<T> {
	pub fn new(value: T) -> Self {
		Self {
			inner: Arc::new(cell::RefCell::new(value)),
		}
	}

	pub fn read(&self) -> OwnedRwLockReadGuardInner<T> {
		let arc = self.inner.clone();
		let guard = arc.borrow();

		// SAFETY: reifydb_single_threaded targets have no real concurrency, so no data race is

		let guard = unsafe { mem::transmute::<Ref<'_, T>, Ref<'static, T>>(guard) };

		OwnedRwLockReadGuardInner {
			_guard: guard,
			_arc: arc,
		}
	}

	pub fn write(&self) -> OwnedRwLockWriteGuardInner<T> {
		let arc = self.inner.clone();
		let guard = arc.borrow_mut();

		// SAFETY: reifydb_single_threaded targets have no real concurrency, so no data race is

		let guard = unsafe { mem::transmute::<RefMut<'_, T>, RefMut<'static, T>>(guard) };

		OwnedRwLockWriteGuardInner {
			_guard: guard,
			_arc: arc,
		}
	}
}

pub struct OwnedRwLockReadGuardInner<T: 'static> {
	_guard: Ref<'static, T>,
	_arc: Arc<cell::RefCell<T>>,
}

impl<T: 'static> Deref for OwnedRwLockReadGuardInner<T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self._guard
	}
}

pub struct OwnedRwLockWriteGuardInner<T: 'static> {
	_guard: RefMut<'static, T>,
	_arc: Arc<cell::RefCell<T>>,
}

impl<T: 'static> Deref for OwnedRwLockWriteGuardInner<T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self._guard
	}
}

impl<T: 'static> DerefMut for OwnedRwLockWriteGuardInner<T> {
	fn deref_mut(&mut self) -> &mut T {
		&mut self._guard
	}
}
