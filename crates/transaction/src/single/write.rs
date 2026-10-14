// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem::take, ops::RangeBounds};

use indexmap::IndexMap;
use reifydb_core::{
	delta::SetPre,
	interface::store::{SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRow},
	key::any::TaggedKey,
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
	pub(super) fetched: HashMap<EncodedKey, Option<EncodedBytes>>,
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
			fetched: HashMap::new(),
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

	pub fn get<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<Option<SingleVersionRow>> {
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

		if let Some(fetched) = self.fetched.get(&encoded) {
			return Ok(fetched.clone().map(|bytes| SingleVersionRow {
				key: encoded,
				bytes,
			}));
		}

		#[cfg(feature = "testing")]
		{
			self.inner.op_counts.lock().unwrap().key(key.clone().into().kind()).gets += 1;
		}
		let store = self.inner.store.read().clone();
		SingleVersionGet::get(&store, &encoded)
	}

	pub fn prefetch<K: Into<TaggedKey> + Clone>(&mut self, keys: &[K]) -> Result<()> {
		let mut missing = Vec::new();
		#[cfg(feature = "testing")]
		let mut kinds = Vec::new();
		for key in keys {
			let key: TaggedKey = key.clone().into();
			let encoded = key.encode();
			self.check_key_allowed(&encoded)?;
			if !self.pending.contains_key(&encoded) && !self.fetched.contains_key(&encoded) {
				self.fetched.insert(encoded.clone(), None);
				missing.push(encoded);
				#[cfg(feature = "testing")]
				kinds.push(key.kind());
			}
		}
		if missing.is_empty() {
			return Ok(());
		}

		#[cfg(feature = "testing")]
		{
			let mut counts = self.inner.op_counts.lock().unwrap();
			counts.get_many_call();
			for kind in kinds {
				counts.key(kind).batched_gets += 1;
			}
		}
		let store = self.inner.store.read().clone();
		for (key, row) in missing.iter().zip(SingleVersionGet::get_many(&store, &missing)?) {
			self.fetched.insert(key.clone(), row.map(|row| row.bytes));
		}
		Ok(())
	}

	pub fn contains_key<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<bool> {
		let tagged: TaggedKey = key.clone().into();
		let key = &tagged.encode();
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

		if let Some(fetched) = self.fetched.get(key) {
			return Ok(fetched.is_some());
		}

		#[cfg(feature = "testing")]
		{
			self.inner.op_counts.lock().unwrap().key(tagged.kind()).contains += 1;
		}
		let store = self.inner.store.read().clone();
		SingleVersionContains::contains(&store, key)
	}

	pub fn set<K: Into<TaggedKey> + Clone>(&mut self, key: &K, bytes: impl Into<EncodedBytes>) -> Result<()> {
		let key: TaggedKey = key.clone().into();
		let encoded = key.encode();
		self.check_key_allowed(&encoded)?;

		let delta = Delta::Set {
			key,
			bytes: bytes.into(),
			pre: SetPre::Unknown,
		};
		self.pending.insert(encoded, delta);
		Ok(())
	}

	pub fn remove_with_pre<K: Into<TaggedKey> + Clone>(&mut self, key: &K, pre: EncodedBytes) -> Result<()> {
		let key: TaggedKey = key.clone().into();
		let encoded = key.encode();
		self.check_key_allowed(&encoded)?;

		self.pending.insert(encoded, Delta::remove_announced(key, pre));
		Ok(())
	}

	pub fn remove<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<()> {
		let key: TaggedKey = key.clone().into();
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
