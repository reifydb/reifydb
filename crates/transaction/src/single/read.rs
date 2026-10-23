// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::store::{SingleVersionGet, SingleVersionRow};
use reifydb_runtime::sync::rwlock::{ArcRwLock, OwnedRwLockReadGuard};
use reifydb_value::{Result, util::hex::encode};

use super::*;
use crate::error::TransactionError;

pub struct KeyReadLock {
	pub(super) _guard: OwnedRwLockReadGuard<()>,
}

impl KeyReadLock {
	pub(super) fn new(lock: ArcRwLock<()>) -> Self {
		Self {
			_guard: lock.read(),
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
				key: encode(key),
			}
			.into())
		}
	}

	pub fn get(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<SingleVersionRow>>> {
		for key in keys {
			self.check_key_allowed(key)?;
		}
		if keys.is_empty() {
			return Ok(Vec::new());
		}
		let store = self.inner.store.read().clone();
		SingleVersionGet::get(&store, keys)
	}
}
