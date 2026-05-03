// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem::{take, transmute};

use indexmap::IndexMap;
use reifydb_core::interface::store::{SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRow};
use reifydb_runtime::sync::rwlock::{RwLock, RwLockWriteGuard};
use reifydb_type::{
	Result,
	util::{cowvec::CowVec, hex},
};

use super::*;
use crate::error::TransactionError;

pub struct KeyWriteLock {
	pub(super) _guard: RwLockWriteGuard<'static, ()>,
	pub(super) _arc: Arc<RwLock<()>>,
}

impl KeyWriteLock {
	pub(super) fn new(arc: Arc<RwLock<()>>) -> Self {
		let guard = arc.write();

		// SAFETY: We're extending the guard's lifetime to 'static.

		let guard = unsafe { transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(guard) };

		Self {
			_arc: arc,
			_guard: guard,
		}
	}
}

pub struct SingleWriteTransaction<'a> {
	pub(super) inner: &'a SingleTransactionInner,
	pub(super) keys: Vec<EncodedKey>,
	pub(super) _key_locks: Vec<KeyWriteLock>,
	pub(super) pending: IndexMap<EncodedKey, Delta>,
	pub(super) completed: bool,
}

impl<'a> SingleWriteTransaction<'a> {
	pub(super) fn new(
		inner: &'a SingleTransactionInner,
		keys: Vec<EncodedKey>,
		key_locks: Vec<KeyWriteLock>,
	) -> Self {
		Self {
			inner,
			keys,
			_key_locks: key_locks,
			pending: IndexMap::new(),
			completed: false,
		}
	}

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
		let deltas: Vec<Delta> = take(&mut self.pending).into_iter().map(|(_, delta)| delta).collect();

		if !deltas.is_empty() {
			let mut store = self.inner.store.write();
			SingleVersionCommit::commit(&mut *store, CowVec::new(deltas))?;
		}

		self.completed = true;
		Ok(())
	}

	pub fn rollback(&mut self) -> Result<()> {
		self.pending.clear();
		self.completed = true;
		Ok(())
	}
}
