// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native mutex implementation using parking_lot.

use std::ops::{Deref, DerefMut};

/// A mutual exclusion primitive for protecting shared data.
///
/// Native implementation wraps parking_lot::Mutex.
pub struct Mutex<T> {
	inner: parking_lot::Mutex<T>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for Mutex<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		self.inner.fmt(f)
	}
}

impl<T> Mutex<T> {
	/// Creates a new mutex.
	pub fn new(value: T) -> Self {
		Self {
			inner: parking_lot::Mutex::new(value),
		}
	}

	/// Acquires the mutex, blocking until it's available.
	pub fn lock(&self) -> MutexGuard<'_, T> {
		MutexGuard {
			inner: self.inner.lock(),
		}
	}

	/// Attempts to acquire the mutex without blocking.
	pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
		self.inner.try_lock().map(|guard| MutexGuard { inner: guard })
	}
}

/// A guard providing mutable access to the data protected by a Mutex.
pub struct MutexGuard<'a, T> {
	pub(in crate::sync) inner: parking_lot::MutexGuard<'a, T>,
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
