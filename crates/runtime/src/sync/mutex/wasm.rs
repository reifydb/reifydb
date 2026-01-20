// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM mutex implementation (no-op).
//!
//! Since WASM is single-threaded, this is a simple wrapper around RefCell.

use std::cell::RefMut;
use std::ops::{Deref, DerefMut};

/// A mutual exclusion primitive for protecting shared data.
///
/// WASM implementation uses RefCell (no actual locking needed).
pub struct Mutex<T> {
	inner: std::cell::RefCell<T>,
}

// SAFETY: WASM is single-threaded, so Sync is safe
unsafe impl<T> Sync for Mutex<T> {}

impl<T: std::fmt::Debug> std::fmt::Debug for Mutex<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Mutex").field("data", &self.inner).finish()
	}
}

impl<T> Mutex<T> {
	/// Creates a new mutex.
	pub fn new(value: T) -> Self {
		Self {
			inner: std::cell::RefCell::new(value),
		}
	}

	/// Acquires the mutex (always succeeds in WASM).
	pub fn lock(&self) -> MutexGuard<'_, T> {
		MutexGuard {
			inner: self.inner.borrow_mut(),
		}
	}

	/// Attempts to acquire the mutex (always succeeds in WASM).
	pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
		self.inner.try_borrow_mut().ok().map(|inner| MutexGuard { inner })
	}
}

/// A guard providing mutable access to the data protected by a Mutex.
pub struct MutexGuard<'a, T> {
	pub(in crate::sync) inner: RefMut<'a, T>,
}

impl<'a, T> Deref for MutexGuard<'a, T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.inner
	}
}

impl<'a, T> DerefMut for MutexGuard<'a, T> {
	fn deref_mut(&mut self) -> &mut T {
		&mut self.inner
	}
}
