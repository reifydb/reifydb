// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use reifydb_core::interface::store::{SingleVersionContains, SingleVersionGet, SingleVersionRow};
use reifydb_runtime::sync::rwlock::{RwLock, RwLockReadGuard};
use reifydb_type::{Result, util::hex};

use super::*;
use crate::error::TransactionError;

/// Holds both the Arc and the guard to keep the lock alive.
/// IMPORTANT: _guard must be declared before _arc so it is dropped first —
/// the guard borrows from the RwLock inside the Arc.
pub struct KeyReadLock {
	pub(super) _guard: RwLockReadGuard<'static, ()>,
	pub(super) _arc: Arc<RwLock<()>>,
}

impl KeyReadLock {
	/// Creates a new KeyReadLock by taking a read guard and storing it with the Arc.
	///
	/// # Safety
	/// This function uses unsafe code to extend the lifetime of the guard to 'static.
	/// This is safe because:
	/// 1. The guard borrows from the RwLock inside the Arc
	/// 2. We store the Arc in this struct, keeping the RwLock alive
	/// 3. The guard will be dropped before or with the Arc (due to field order)
	/// 4. As long as this struct exists, the Arc exists, so the RwLock exists
	pub(super) fn new(arc: Arc<RwLock<()>>) -> Self {
		// Take the guard while we still have a reference to arc
		let guard = arc.read();

		// SAFETY: We're extending the guard's lifetime to 'static.
		// This is sound because we're also storing the Arc, which keeps
		// the underlying RwLock alive for as long as this struct exists.
		let guard = unsafe { mem::transmute::<RwLockReadGuard<'_, ()>, RwLockReadGuard<'static, ()>>(guard) };

		Self {
			_arc: arc,
			_guard: guard,
		}
	}
}

pub struct SingleReadTransaction<'a> {
	pub(super) inner: &'a SingleTransactionInner,
	pub(super) keys: Vec<EncodedKey>,
	pub(super) _key_locks: Vec<KeyReadLock>,
}

impl<'a> SingleReadTransaction<'a> {
	#[inline]
	fn check_key_allowed(&self, key: &EncodedKey) -> Result<()> {
		if self.keys.iter().any(|k| k == key) {
			Ok(())
		} else {
			Err(TransactionError::KeyOutOfScope {
				key: hex::encode(key),
			}
			.into())
		}
	}

	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<SingleVersionRow>> {
		self.check_key_allowed(key)?;
		let store = self.inner.store.read().clone();
		SingleVersionGet::get(&store, key)
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.check_key_allowed(key)?;
		let store = self.inner.store.read().clone();
		SingleVersionContains::contains(&store, key)
	}
}
