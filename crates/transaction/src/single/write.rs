// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::take, ops::RangeBounds};

use indexmap::IndexMap;
use reifydb_core::interface::store::{SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRow};
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

	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<SingleVersionRow>> {
		self.check_key_allowed(key)?;

		if let Some(delta) = self.pending.get(key) {
			return match delta {
				Delta::Set {
					bytes,
					..
				} => Ok(Some(SingleVersionRow {
					key: key.clone(),
					bytes: bytes.clone(),
				})),
				Delta::Remove {
					..
				} => Ok(None),
			};
		}

		let store = self.inner.store.read().clone();
		SingleVersionGet::get(&store, key)
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
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

	pub fn set(&mut self, key: &EncodedKey, bytes: impl Into<EncodedBytes>) -> Result<()> {
		self.check_key_allowed(key)?;

		let delta = Delta::Set {
			key: key.clone(),
			bytes: bytes.into(),
		};
		self.pending.insert(key.clone(), delta);
		Ok(())
	}

	pub fn remove_with_pre(&mut self, key: &EncodedKey, pre: EncodedBytes) -> Result<()> {
		self.check_key_allowed(key)?;

		self.pending.insert(key.clone(), Delta::remove_announced(key.clone(), pre));
		Ok(())
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.check_key_allowed(key)?;

		self.pending.insert(key.clone(), Delta::remove_silent(key.clone()));
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
