// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use reifydb_core::interface::store::{SingleVersionContains, SingleVersionGet, SingleVersionRow};
use reifydb_runtime::sync::rwlock::{RwLock, RwLockReadGuard};
use reifydb_type::{Result, util::hex};

use super::*;
use crate::error::TransactionError;

pub struct KeyReadLock {
	pub(super) _guard: RwLockReadGuard<'static, ()>,
	pub(super) _arc: Arc<RwLock<()>>,
}

impl KeyReadLock {
	pub(super) fn new(arc: Arc<RwLock<()>>) -> Self {
		let guard = arc.read();

		// SAFETY: We're extending the guard's lifetime to 'static.

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
