// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use diagnostic::transaction::key_out_of_scope;
use reifydb_core::interface::SingleVersionQueryTransaction;
use reifydb_store_transaction::{SingleVersionContains, SingleVersionGet};
use reifydb_type::{diagnostic, error, util::hex};
use tokio::sync::OwnedRwLockReadGuard;

use super::*;

/// Simple wrapper around tokio's OwnedRwLockReadGuard.
/// OwnedRwLockReadGuard is Send, so this struct is Send.
#[allow(dead_code)]
pub struct KeyReadLock(pub(super) OwnedRwLockReadGuard<()>);

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

#[async_trait]
impl SingleVersionQueryTransaction for SvlQueryTransaction<'_> {
	async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		self.check_key_allowed(key)?;
		// Clone the store to avoid holding the lock across await
		// TransactionStore is Arc-based, so clone is cheap
		let store = self.inner.store.read().await.clone();
		SingleVersionGet::get(&store, key).await
	}

	async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.check_key_allowed(key)?;
		// Clone the store to avoid holding the lock across await
		let store = self.inner.store.read().await.clone();
		SingleVersionContains::contains(&store, key).await
	}
}
