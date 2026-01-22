// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM mutex implementation (no-op).
//!
//! Since WASM is single-threaded, this is a simple wrapper around RefCell.

use std::cell::RefMut;
use std::ops::{Deref, DerefMut};

/// WASM mutex implementation using RefCell (no actual locking needed).
pub struct MutexInner<T> {
	inner: std::cell::RefCell<T>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for MutexInner<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MutexInner").field("data", &self.inner).finish()
	}
}

impl<T> MutexInner<T> {
	/// Creates a new mutex.
	pub fn new(value: T) -> Self {
		Self {
			inner: std::cell::RefCell::new(value),
		}
	}

	/// Acquires the mutex (always succeeds in WASM).
	pub fn lock(&self) -> MutexGuardInner<'_, T> {
		MutexGuardInner {
			inner: self.inner.borrow_mut(),
		}
	}

	/// Attempts to acquire the mutex (always succeeds in WASM).
	pub fn try_lock(&self) -> Option<MutexGuardInner<'_, T>> {
		self.inner.try_borrow_mut().ok().map(|inner| MutexGuardInner { inner })
	}
}

/// WASM guard providing mutable access to the data protected by a Mutex.
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
