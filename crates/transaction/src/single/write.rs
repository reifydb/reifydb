// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::take, ops::RangeBounds};

use indexmap::IndexMap;
use reifydb_core::interface::store::{SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRow};
use reifydb_runtime::sync::rwlock::{ArcRwLock, OwnedRwLockWriteGuard};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sub_raft::message::Command;
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
					row,
					..
				} => Ok(Some(SingleVersionRow {
					key: key.clone(),
					row: row.clone(),
				})),
				Delta::Unset {
					..
				}
				| Delta::Remove {
					..
				}
				| Delta::Drop {
					key: _,
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
				Delta::Unset {
					..
				}
				| Delta::Remove {
					..
				}
				| Delta::Drop {
					key: _,
				} => Ok(false),
			};
		}

		let store = self.inner.store.read().clone();
		SingleVersionContains::contains(&store, key)
	}

	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.check_key_allowed(key)?;

		let delta = Delta::Set {
			key: key.clone(),
			row,
		};
		self.pending.insert(key.clone(), delta);
		Ok(())
	}

	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.check_key_allowed(key)?;

		self.pending.insert(
			key.clone(),
			Delta::Unset {
				key: key.clone(),
				row,
			},
		);
		Ok(())
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.check_key_allowed(key)?;

		self.pending.insert(
			key.clone(),
			Delta::Remove {
				key: key.clone(),
			},
		);
		Ok(())
	}

	pub fn commit(&mut self) -> Result<()> {
		let deltas = self.drain_pending();

		if !deltas.is_empty() {
			self.propose_or_commit(deltas)?;
		}

		self.completed = true;
		Ok(())
	}

	#[inline]
	fn drain_pending(&mut self) -> Vec<Delta> {
		take(&mut self.pending).into_iter().map(|(_, delta)| delta).collect()
	}

	#[inline]
	fn propose_or_commit(&self, deltas: Vec<Delta>) -> Result<()> {
		reifydb_assertions! {
			let count = deltas.len();
			assert!(
				count > 0,
				"propose_or_commit must not run on an empty delta set; an empty raft proposal \
				 or store commit acquires a lock and emits a needless command for a no-op \
				 transaction (count={count})"
			);
		}

		#[cfg(not(target_arch = "wasm32"))]
		{
			let raft_handle = self.inner.raft.read().clone();
			if let Some(raft) = raft_handle {
				let cmd = Command::WriteSingle {
					deltas,
				};
				raft.propose(cmd).map_err(|e| TransactionError::RaftProposeFailed {
					message: e.to_string(),
				})?;
				return Ok(());
			}
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
