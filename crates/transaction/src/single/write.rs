// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem::take, ops::RangeBounds};

use indexmap::IndexMap;
use reifydb_core::{
	delta::SetPre,
	interface::store::{SingleVersionGet, SingleVersionRow},
	key::any::TaggedKey,
	wal::SingleRecord,
};
use reifydb_runtime::sync::rwlock::{ArcRwLock, OwnedRwLockWriteGuard};
use reifydb_value::{
	Result, reifydb_assertions,
	util::{cowvec::CowVec, hex::encode},
};

use super::*;
use crate::{error::TransactionError, wal::check_record_size};

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

	pub fn get<K: Into<TaggedKey> + Clone>(&mut self, keys: &[K]) -> Result<Vec<Option<SingleVersionRow>>> {
		let mut encoded_keys = Vec::with_capacity(keys.len());
		let mut missing = Vec::new();
		#[cfg(feature = "testing")]
		let mut kinds = Vec::new();
		for key in keys {
			let key: TaggedKey = key.clone().into();
			let encoded = key.encode();
			self.check_key_allowed(&encoded)?;
			if !self.pending.contains_key(&encoded) && !self.fetched.contains_key(&encoded) {
				self.fetched.insert(encoded.clone(), None);
				missing.push(encoded.clone());
				#[cfg(feature = "testing")]
				kinds.push(key.kind());
			}
			encoded_keys.push(encoded);
		}

		if !missing.is_empty() {
			#[cfg(feature = "testing")]
			{
				let mut counts = self.inner.op_counts.lock().unwrap();
				counts.get_call(kinds);
			}
			let store = self.inner.store.read().clone();
			for (key, row) in missing.iter().zip(SingleVersionGet::get(&store, &missing)?) {
				self.fetched.insert(key.clone(), row.map(|row| row.bytes));
			}
		}

		Ok(encoded_keys.into_iter().map(|encoded| self.lookup(encoded)).collect())
	}

	fn lookup(&self, encoded: EncodedKey) -> Option<SingleVersionRow> {
		if let Some(delta) = self.pending.get(&encoded) {
			return match delta {
				Delta::Set {
					bytes,
					..
				} => Some(SingleVersionRow {
					key: encoded,
					bytes: bytes.clone(),
				}),
				Delta::Remove {
					..
				} => None,
			};
		}
		self.fetched.get(&encoded).cloned().flatten().map(|bytes| SingleVersionRow {
			key: encoded,
			bytes,
		})
	}

	pub fn set<K: Into<TaggedKey> + Clone, B: Into<EncodedBytes>>(&mut self, entries: Vec<(K, B)>) -> Result<()> {
		let entries = self.scoped(entries)?;
		for (key, encoded, bytes) in entries {
			let delta = Delta::Set {
				key,
				bytes: bytes.into(),
				pre: SetPre::Unknown,
			};
			self.pending.insert(encoded, delta);
		}
		Ok(())
	}

	pub fn remove_with_pre<K: Into<TaggedKey> + Clone, B: Into<EncodedBytes>>(
		&mut self,
		entries: Vec<(K, B)>,
	) -> Result<()> {
		let entries = self.scoped(entries)?;
		for (key, encoded, pre) in entries {
			self.pending.insert(encoded, Delta::remove_announced(key, pre.into()));
		}
		Ok(())
	}

	pub fn remove<K: Into<TaggedKey> + Clone>(&mut self, keys: &[K]) -> Result<()> {
		let entries = self.scoped(keys.iter().map(|key| (key.clone(), ())).collect())?;
		for (key, encoded, ()) in entries {
			self.pending.insert(encoded, Delta::remove_silent(key));
		}
		Ok(())
	}

	fn scoped<K: Into<TaggedKey>, V>(&self, entries: Vec<(K, V)>) -> Result<Vec<(TaggedKey, EncodedKey, V)>> {
		let mut scoped = Vec::with_capacity(entries.len());
		for (key, value) in entries {
			let key: TaggedKey = key.into();
			let encoded = key.encode();
			self.check_key_allowed(&encoded)?;
			scoped.push((key, encoded, value));
		}
		Ok(scoped)
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

		let mut encoded = Vec::new();
		SingleRecord::encode_deltas(&deltas, &mut encoded);
		check_record_size(&encoded)?;
		let store = self.inner.store.write();
		let lsn = self.inner.wal.append_encoded(self.inner.clock.now(), SingleRecord::KIND, encoded).unwrap_or_else(
			|err| panic!("single commit of {} deltas failed to append to the commit log: {err}", deltas.len()),
		);
		if let Err(err) = store.commit_logged(CowVec::new(deltas), lsn) {
			panic!("single commit is in the commit log but failed to install in the store: {err}");
		}
		Ok(())
	}

	pub fn rollback(&mut self) -> Result<()> {
		self.pending.clear();
		self.completed = true;
		Ok(())
	}
}
