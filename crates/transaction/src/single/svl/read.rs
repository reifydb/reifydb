// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use diagnostic::transaction::key_out_of_scope;
use parking_lot::{RwLock, RwLockReadGuard};
use reifydb_core::interface::SingleVersionValues;
use reifydb_core::interface::{SingleVersionContains, SingleVersionGet};
use reifydb_type::{diagnostic, error, util::hex};

use super::*;

/// Holds both the Arc and the guard to keep the lock alive
#[allow(dead_code)]
pub struct KeyReadLock {
	pub(super) _arc: Arc<RwLock<()>>,
	pub(super) _guard: RwLockReadGuard<'static, ()>,
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
		let guard =
			unsafe { std::mem::transmute::<RwLockReadGuard<'_, ()>, RwLockReadGuard<'static, ()>>(guard) };

		Self {
			_arc: arc,
			_guard: guard,
		}
	}
}

pub struct SvlQueryTransaction<'a> {
	pub(super) inner: &'a TransactionSvlInner,
	pub(super) keys: Vec<EncodedKey>,
	pub(super) _key_locks: Vec<KeyReadLock>,
}

impl<'a> SvlQueryTransaction<'a> {
	#[inline]
	fn check_key_allowed(&self, key: &EncodedKey) -> crate::Result<()> {
		if self.keys.iter().any(|k| k == key) {
			Ok(())
		} else {
			Err(error!(key_out_of_scope(hex::encode(&key))))
		}
	}

	pub fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		self.check_key_allowed(key)?;
		let store = self.inner.store.read().clone();
		SingleVersionGet::get(&store, key)
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.check_key_allowed(key)?;
		let store = self.inner.store.read().clone();
		SingleVersionContains::contains(&store, key)
	}
}
