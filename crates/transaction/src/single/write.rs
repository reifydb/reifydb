// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::take, ops::RangeBounds};

use indexmap::IndexMap;
use reifydb_core::{
	interface::store::{SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRow},
	key::any::AnyKey,
};
use reifydb_runtime::sync::rwlock::{ArcRwLock, OwnedRwLockWriteGuard};
use reifydb_value::{
	Result, reifydb_assertions,
	util::{cowvec::CowVec, hex::encode},
};

use super::*;
use crate::error::TransactionError;

pub struct KeyWriteLock {
	pub(super) _guard: OwnedRwLockWriteGuard<()>,
}

impl KeyWriteLock {
	pub(super) fn new(lock: ArcRwLock<()>) -> Self {
		Self {
			_guard: lock.write(),
		}
	}
}

pub struct SingleWriteTransaction<'a> {
	pub(super) inner: &'a SingleTransactionInner,
	pub(super) keys: Vec<EncodedKey>,
	pub(super) ranges: Vec<EncodedKeyRange>,
	pub(super) _key_locks: Vec<KeyWriteLock>,
	pub(super) pending: IndexMap<EncodedKey, Delta>,
	pub(super) completed: bool,
}

impl<'a> SingleWriteTransaction<'a> {
	pub(super) fn new(
		inner: &'a SingleTransactionInner,
		keys: Vec<EncodedKey>,
		ranges: Vec<EncodedKeyRange>,
		key_locks: Vec<KeyWriteLock>,
	) -> Self {
		Self {
			inner,
			keys,
			ranges,
			_key_locks: key_locks,
			pending: IndexMap::new(),
			completed: false,
		}
	}

	#[inline]
	fn check_key_allowed(&self, key: &EncodedKey) -> Result<()> {
		if self.keys.iter().any(|k| k == key) || self.ranges.iter().any(|range| range.contains(key)) {
			Ok(())
		} else {
			Err(TransactionError::KeyOutOfScope {
				key: encode(key),
			}
			.into())
		}
	}

	pub fn get<K: Into<AnyKey> + Clone>(&mut self, key: &K) -> Result<Option<SingleVersionRow>> {
		let encoded = key.clone().into().encode();
		self.check_key_allowed(&encoded)?;

		if let Some(delta) = self.pending.get(&encoded) {
			return match delta {
				Delta::Set {
					bytes,
					..
				} => Ok(Some(SingleVersionRow {
					key: encoded,
					bytes: bytes.clone(),
				})),
				Delta::Remove {
					..
				} => Ok(None),
			};
		}

		let store = self.inner.store.read().clone();
		SingleVersionGet::get(&store, &encoded)
	}

	pub fn contains_key<K: Into<AnyKey> + Clone>(&mut self, key: &K) -> Result<bool> {
		let key = &key.clone().into().encode();
		self.check_key_allowed(key)?;

		if let Some(delta) = self.pending.get(key) {
			return match delta {
				Delta::Set {
					..
				} => Ok(true),
				Delta::Remove {
					..
				} => Ok(false),
			};
		}

		let store = self.inner.store.read().clone();
		SingleVersionContains::contains(&store, key)
	}

	pub fn set<K: Into<AnyKey> + Clone>(&mut self, key: &K, bytes: impl Into<EncodedBytes>) -> Result<()> {
		let key: AnyKey = key.clone().into();
		let encoded = key.encode();
		self.check_key_allowed(&encoded)?;

		let delta = Delta::Set {
			key,
			bytes: bytes.into(),
		};
		self.pending.insert(encoded, delta);
		Ok(())
	}

	pub fn remove_with_pre<K: Into<AnyKey> + Clone>(&mut self, key: &K, pre: EncodedBytes) -> Result<()> {
		let key: AnyKey = key.clone().into();
		let encoded = key.encode();
		self.check_key_allowed(&encoded)?;

		self.pending.insert(encoded, Delta::remove_announced(key, pre));
		Ok(())
	}

	pub fn remove<K: Into<AnyKey> + Clone>(&mut self, key: &K) -> Result<()> {
		let key: AnyKey = key.clone().into();
		let encoded = key.encode();
		self.check_key_allowed(&encoded)?;

		self.pending.insert(encoded, Delta::remove_silent(key));
		Ok(())
	}

	pub fn commit(&mut self) -> Result<()> {
		let deltas = self.drain_pending();

		if !deltas.is_empty() {
			self.commit_deltas(deltas)?;
		}

		self.completed = true;
		Ok(())
	}

	#[inline]
	fn drain_pending(&mut self) -> Vec<Delta> {
		take(&mut self.pending).into_iter().map(|(_, delta)| delta).collect()
	}

	#[inline]
	fn commit_deltas(&self, deltas: Vec<Delta>) -> Result<()> {
		reifydb_assertions! {
			let count = deltas.len();
			assert!(
				count > 0,
				"commit_deltas must not run on an empty delta set; an empty store commit \
				 acquires the store write lock for a no-op transaction (count={count})"
			);
		}

		let mut store = self.inner.store.write();
		SingleVersionCommit::commit(&mut *store, CowVec::new(deltas))
	}

	pub fn rollback(&mut self) -> Result<()> {
		self.pending.clear();
		self.completed = true;
		Ok(())
	}
}
