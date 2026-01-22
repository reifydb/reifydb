// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::Debug;
use cfg_if::cfg_if;
use std::ops::{Deref, DerefMut};

#[cfg(reifydb_target = "native")]
pub(crate) mod native;
#[cfg(reifydb_target = "wasm")]
pub(crate) mod wasm;

cfg_if! {
	if #[cfg(reifydb_target = "native")] {
		type MutexInnerImpl<T> = native::MutexInner<T>;
		type MutexGuardInnerImpl<'a, T> = native::MutexGuardInner<'a, T>;
	} else {
		type MutexInnerImpl<T> = wasm::MutexInner<T>;
		type MutexGuardInnerImpl<'a, T> = wasm::MutexGuardInner<'a, T>;
	}
}

/// A mutual exclusion primitive for protecting shared data.
pub struct Mutex<T> {
	inner: MutexInnerImpl<T>,
}

impl<T: Debug> Debug for Mutex<T> {
	#[inline]
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		self.inner.fmt(f)
	}
}

// SAFETY: WASM is single-threaded, so Sync is safe
#[cfg(reifydb_target = "wasm")]
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
