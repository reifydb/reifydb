// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use diagnostic::transaction::key_out_of_scope;
use parking_lot::{RwLock as ParkingRwLock, RwLockReadGuard as ParkingRwLockReadGuard};
use reifydb_core::interface::SingleVersionQueryTransaction;
use reifydb_store_transaction::{SingleVersionContains, SingleVersionGet};
use reifydb_type::{diagnostic, error, util::hex};
use self_cell::self_cell;

use super::*;

// Type alias for the read guard
type ReadGuard<'a> = ParkingRwLockReadGuard<'a, ()>;

// Safe self-referential struct that owns both the Arc and the guard borrowing from it
self_cell! {
	pub struct KeyReadLock {
		owner: Arc<ParkingRwLock<()>>,

		#[covariant]
		dependent: ReadGuard,
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
}

impl SingleVersionQueryTransaction for SvlQueryTransaction<'_> {
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		self.check_key_allowed(key)?;
		let store = self.inner.store.read().unwrap();
		store.get(key)
	}

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.check_key_allowed(key)?;
		let store = self.inner.store.read().unwrap();
		store.contains(key)
	}
}
