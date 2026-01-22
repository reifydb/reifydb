// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::mem::take;

use indexmap::IndexMap;
use reifydb_core::{
	error::diagnostic::transaction::key_out_of_scope,
	interface::store::{SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionValues},
};
use reifydb_runtime::sync::rwlock::{RwLock, RwLockWriteGuard};
use reifydb_type::{
	error,
	util::{cowvec::CowVec, hex},
};

use super::*;

/// Holds both the Arc and the guard to keep the lock alive
#[allow(dead_code)]
pub struct KeyWriteLock {
	pub(super) _arc: Arc<RwLock<()>>,
	pub(super) _guard: RwLockWriteGuard<'static, ()>,
}

impl KeyWriteLock {
	/// Creates a new KeyWriteLock by taking a write guard and storing it with the Arc.
	///
	/// # Safety
	/// This function uses unsafe code to extend the lifetime of the guard to 'static.
	/// This is safe because:
	/// 1. The guard borrows from the RwLock inside the Arc
	/// 2. We store the Arc in this struct, keeping the RwLock alive
	/// 3. The guard will be dropped before or with the Arc (due to field order)
	/// 4. As long as this struct exists, the Arc exists, so the RwLock exists
	pub(super) fn new(arc: Arc<RwLock<()>>) -> Self {
		// Take the guard while we still have a reference to arc
		let guard = arc.write();

		// SAFETY: We're extending the guard's lifetime to 'static.
		// This is sound because we're also storing the Arc, which keeps
		// the underlying RwLock alive for as long as this struct exists.
		let guard = unsafe {
			std::mem::transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(guard)
		};

		Self {
			_arc: arc,
			_guard: guard,
		}
	}
}

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
	fn check_key_allowed(&self, key: &EncodedKey) -> reifydb_type::Result<()> {
		if self.keys.iter().any(|k| k == key) {
			Ok(())
		} else {
			Err(error!(key_out_of_scope(hex::encode(&key))))
		}
	}

	pub fn get(&mut self, key: &EncodedKey) -> reifydb_type::Result<Option<SingleVersionValues>> {
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
				Delta::Unset {
					..
				}
				| Delta::Remove {
					..
				}
				| Delta::Drop {
					..
				} => Ok(None),
			};
		}

		// Clone the store to avoid holding the lock
		// TransactionStore is Arc-based, so clone is cheap
		let store = self.inner.store.read().clone();
		SingleVersionGet::get(&store, key)
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> reifydb_type::Result<bool> {
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
					..
				} => Ok(false),
			};
		}

		// Clone the store to avoid holding the lock
		let store = self.inner.store.read().clone();
		SingleVersionContains::contains(&store, key)
	}

	pub fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> reifydb_type::Result<()> {
		self.check_key_allowed(key)?;

		let delta = Delta::Set {
			key: key.clone(),
			values,
		};
		self.pending.insert(key.clone(), delta);
		Ok(())
	}

	pub fn unset(&mut self, key: &EncodedKey, values: EncodedValues) -> reifydb_type::Result<()> {
		self.check_key_allowed(key)?;

		self.pending.insert(
			key.clone(),
			Delta::Unset {
				key: key.clone(),
				values,
			},
		);
		Ok(())
	}

	pub fn remove(&mut self, key: &EncodedKey) -> reifydb_type::Result<()> {
		self.check_key_allowed(key)?;

		self.pending.insert(
			key.clone(),
			Delta::Remove {
				key: key.clone(),
			},
		);
		Ok(())
	}

	pub fn commit(&mut self) -> reifydb_type::Result<()> {
		let deltas: Vec<Delta> = take(&mut self.pending).into_iter().map(|(_, delta)| delta).collect();

		if !deltas.is_empty() {
			let mut store = self.inner.store.write();
			SingleVersionCommit::commit(&mut *store, CowVec::new(deltas))?;
		}

		self.completed = true;
		Ok(())
	}

	pub fn rollback(&mut self) -> reifydb_type::Result<()> {
		self.pending.clear();
		self.completed = true;
		Ok(())
	}
}
