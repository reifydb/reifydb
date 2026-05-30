// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::Debug,
	ops::{Deref, DerefMut},
};

use cfg_if::cfg_if;

#[cfg(not(reifydb_single_threaded))]
pub(crate) mod native;
#[cfg(reifydb_single_threaded)]
pub(crate) mod wasm;

cfg_if! {
	if #[cfg(not(reifydb_single_threaded))] {
		type RwLockInnerImpl<T> = native::RwLockInner<T>;
		type RwLockReadGuardInnerImpl<'a, T> = native::RwLockReadGuardInner<'a, T>;
		type RwLockWriteGuardInnerImpl<'a, T> = native::RwLockWriteGuardInner<'a, T>;
		type ArcRwLockInnerImpl<T> = native::ArcRwLockInner<T>;
		type OwnedRwLockReadGuardInnerImpl<T> = native::OwnedRwLockReadGuardInner<T>;
		type OwnedRwLockWriteGuardInnerImpl<T> = native::OwnedRwLockWriteGuardInner<T>;
	} else {
		type RwLockInnerImpl<T> = wasm::RwLockInner<T>;
		type RwLockReadGuardInnerImpl<'a, T> = wasm::RwLockReadGuardInner<'a, T>;
		type RwLockWriteGuardInnerImpl<'a, T> = wasm::RwLockWriteGuardInner<'a, T>;
		type ArcRwLockInnerImpl<T> = wasm::ArcRwLockInner<T>;
		type OwnedRwLockReadGuardInnerImpl<T> = wasm::OwnedRwLockReadGuardInner<T>;
		type OwnedRwLockWriteGuardInnerImpl<T> = wasm::OwnedRwLockWriteGuardInner<T>;
	}
}

pub struct RwLock<T> {
	inner: RwLockInnerImpl<T>,
}

impl<T: Debug> Debug for RwLock<T> {
	#[inline]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.inner.fmt(f)
	}
}

// SAFETY: Single-threaded targets (WASM/WASI) don't have real concurrency
#[cfg(reifydb_single_threaded)]
unsafe impl<T> Sync for RwLock<T> {}

impl<T> RwLock<T> {
	#[inline]
	pub fn new(value: T) -> Self {
		Self {
			inner: RwLockInnerImpl::new(value),
		}
	}

	#[inline]
	pub fn read(&self) -> RwLockReadGuard<'_, T> {
		RwLockReadGuard {
			inner: self.inner.read(),
		}
	}

	#[inline]
	pub fn write(&self) -> RwLockWriteGuard<'_, T> {
		RwLockWriteGuard {
			inner: self.inner.write(),
		}
	}

	#[inline]
	pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
		self.inner.try_read().map(|inner| RwLockReadGuard {
			inner,
		})
	}

	#[inline]
	pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
		self.inner.try_write().map(|inner| RwLockWriteGuard {
			inner,
		})
	}

	#[inline]
	pub fn read_recursive(&self) -> RwLockReadGuard<'_, T> {
		RwLockReadGuard {
			inner: self.inner.read_recursive(),
		}
	}

	#[inline]
	pub fn try_read_recursive(&self) -> Option<RwLockReadGuard<'_, T>> {
		self.inner.try_read_recursive().map(|inner| RwLockReadGuard {
			inner,
		})
	}
}

impl<T: Default> Default for RwLock<T> {
	#[inline]
	fn default() -> Self {
		Self::new(T::default())
	}
}

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

impl<'a, T: Debug> Debug for RwLockReadGuard<'a, T> {
	#[inline]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		(**self).fmt(f)
	}
}

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

impl<'a, T: Debug> Debug for RwLockWriteGuard<'a, T> {
	#[inline]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		(**self).fmt(f)
	}
}

pub struct ArcRwLock<T> {
	inner: ArcRwLockInnerImpl<T>,
}

impl<T> Clone for ArcRwLock<T> {
	#[inline]
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

#[cfg(reifydb_single_threaded)]
unsafe impl<T> Send for ArcRwLock<T> {}
#[cfg(reifydb_single_threaded)]
unsafe impl<T> Sync for ArcRwLock<T> {}

impl<T: 'static> ArcRwLock<T> {
	#[inline]
	pub fn new(value: T) -> Self {
		Self {
			inner: ArcRwLockInnerImpl::new(value),
		}
	}

	#[inline]
	pub fn read(&self) -> OwnedRwLockReadGuard<T> {
		OwnedRwLockReadGuard {
			inner: self.inner.read(),
		}
	}

	#[inline]
	pub fn write(&self) -> OwnedRwLockWriteGuard<T> {
		OwnedRwLockWriteGuard {
			inner: self.inner.write(),
		}
	}
}

pub struct OwnedRwLockReadGuard<T: 'static> {
	inner: OwnedRwLockReadGuardInnerImpl<T>,
}

impl<T: 'static> Deref for OwnedRwLockReadGuard<T> {
	type Target = T;

	#[inline]
	fn deref(&self) -> &T {
		&self.inner
	}
}

pub struct OwnedRwLockWriteGuard<T: 'static> {
	inner: OwnedRwLockWriteGuardInnerImpl<T>,
}

impl<T: 'static> Deref for OwnedRwLockWriteGuard<T> {
	type Target = T;

	#[inline]
	fn deref(&self) -> &T {
		&self.inner
	}
}

impl<T: 'static> DerefMut for OwnedRwLockWriteGuard<T> {
	#[inline]
	fn deref_mut(&mut self) -> &mut T {
		&mut self.inner
	}
}
