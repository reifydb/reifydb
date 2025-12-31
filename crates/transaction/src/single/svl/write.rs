// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::mem::take;

use indexmap::IndexMap;
use reifydb_core::interface::SingleVersionValues;
use reifydb_store_transaction::{SingleVersionCommit, SingleVersionContains, SingleVersionGet};
use reifydb_type::{diagnostic::transaction::key_out_of_scope, error, util::hex};
use tokio::sync::OwnedRwLockWriteGuard;
use tracing::{Instrument, debug_span};

use super::*;

/// Simple wrapper around tokio's OwnedRwLockWriteGuard.
/// OwnedRwLockWriteGuard is Send, so this struct is Send.
#[allow(dead_code)]
pub struct KeyWriteLock(pub(super) OwnedRwLockWriteGuard<()>);

pub struct SvlCommandTransaction<'a> {
	pub(super) inner: &'a TransactionSvlInner,
	pub(super) keys: Vec<EncodedKey>,
	pub(super) _key_locks: Vec<KeyWriteLock>,
	pub(super) pending: IndexMap<EncodedKey, Delta>,
	pub(super) completed: bool,
}

impl<'a> SvlCommandTransaction<'a> {
	pub(super) fn new(inner: &'a TransactionSvlInner, keys: Vec<EncodedKey>, key_locks: Vec<KeyWriteLock>) -> Self {
		Self {
			inner,
			keys,
			_key_locks: key_locks,
			pending: IndexMap::new(),
			completed: false,
		}
	}

	#[inline]
	fn check_key_allowed(&self, key: &EncodedKey) -> crate::Result<()> {
		if self.keys.iter().any(|k| k == key) {
			Ok(())
		} else {
			Err(error!(key_out_of_scope(hex::encode(&key))))
		}
	}

	pub async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		self.check_key_allowed(key)?;

		if let Some(delta) = self.pending.get(key) {
			return match delta {
				Delta::Set {
					values,
					..
				} => Ok(Some(SingleVersionValues {
					key: key.clone(),
					values: values.clone(),
				})),
				Delta::Remove {
					..
				} => Ok(None),
				Delta::Drop {
					..
				} => Ok(None),
			};
		}

		// Clone the store to avoid holding the lock across await
		// TransactionStore is Arc-based, so clone is cheap
		let store = self.inner.store.read().await.clone();
		SingleVersionGet::get(&store, key).instrument(debug_span!("svl_get_from_store")).await
	}

	pub async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.check_key_allowed(key)?;

		if let Some(delta) = self.pending.get(key) {
			return match delta {
				Delta::Set {
					..
				} => Ok(true),
				Delta::Remove {
					..
				} => Ok(false),
				Delta::Drop {
					..
				} => Ok(false),
			};
		}

		// Clone the store to avoid holding the lock across await
		let store = self.inner.store.read().await.clone();
		SingleVersionContains::contains(&store, key).await
	}

	pub fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> crate::Result<()> {
		self.check_key_allowed(key)?;

		let delta = Delta::Set {
			key: key.clone(),
			values,
		};
		self.pending.insert(key.clone(), delta);
		Ok(())
	}

	pub async fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.check_key_allowed(key)?;

		self.pending.insert(
			key.clone(),
			Delta::Remove {
				key: key.clone(),
			},
		);
		Ok(())
	}

	pub async fn commit(&mut self) -> crate::Result<()> {
		let deltas: Vec<Delta> = take(&mut self.pending).into_iter().map(|(_, delta)| delta).collect();

		if !deltas.is_empty() {
			let mut store = self.inner.store.write().await;
			SingleVersionCommit::commit(&mut *store, CowVec::new(deltas))
				.instrument(debug_span!("svl_store_commit"))
				.await?;
		}

		self.completed = true;
		Ok(())
	}

	pub async fn rollback(&mut self) -> crate::Result<()> {
		self.pending.clear();
		self.completed = true;
		Ok(())
	}
}
