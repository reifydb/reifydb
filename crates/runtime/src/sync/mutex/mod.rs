// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::Debug,
	ops::{Deref, DerefMut},
};

use cfg_if::cfg_if;

#[cfg(all(not(reifydb_single_threaded), not(loom)))]
pub(crate) mod host;
#[cfg(loom)]
pub(crate) mod loom;
#[cfg(reifydb_single_threaded)]
pub(crate) mod wasm;

cfg_if! {
	if #[cfg(loom)] {
		type MutexInnerImpl<T> = loom::MutexInner<T>;
		type MutexGuardInnerImpl<'a, T> = loom::MutexGuardInner<'a, T>;
	} else if #[cfg(not(reifydb_single_threaded))] {
		type MutexInnerImpl<T> = host::MutexInner<T>;
		type MutexGuardInnerImpl<'a, T> = host::MutexGuardInner<'a, T>;
	} else {
		type MutexInnerImpl<T> = wasm::MutexInner<T>;
		type MutexGuardInnerImpl<'a, T> = wasm::MutexGuardInner<'a, T>;
	}
}

pub struct Mutex<T> {
	inner: MutexInnerImpl<T>,
}

impl<T: Debug> Debug for Mutex<T> {
	#[inline]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.inner.fmt(f)
	}
}

// SAFETY: under reifydb_single_threaded there is no second thread, so the RefCell-backed inner can never
// be reached concurrently. The impl only satisfies Sync bounds and never backs a real cross-thread share.
#[cfg(reifydb_single_threaded)]
unsafe impl<T> Sync for Mutex<T> {}

impl<T> Mutex<T> {
	#[inline]
	pub fn new(value: T) -> Self {
		Self {
			inner: MutexInnerImpl::new(value),
		}
	}

	#[inline]
	pub fn lock(&self) -> MutexGuard<'_, T> {
		MutexGuard {
			inner: self.inner.lock(),
		}
	}

	#[inline]
	pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
		self.inner.try_lock().map(|inner| MutexGuard {
			inner,
		})
	}
}

impl<T: Default> Default for Mutex<T> {
	#[inline]
	fn default() -> Self {
		Self::new(T::default())
	}
}

pub struct MutexGuard<'a, T> {
	pub(in crate::sync) inner: MutexGuardInnerImpl<'a, T>,
}

impl<'a, T> Deref for MutexGuard<'a, T> {
	type Target = T;

	#[inline]
	fn deref(&self) -> &T {
		&self.inner
	}
}

impl<'a, T> DerefMut for MutexGuard<'a, T> {
	#[inline]
	fn deref_mut(&mut self) -> &mut T {
		&mut self.inner
	}
}

impl<'a, T: Debug> Debug for MutexGuard<'a, T> {
	#[inline]
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		(**self).fmt(f)
	}
}
