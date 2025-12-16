// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::mem::take;

use indexmap::IndexMap;
use parking_lot::{RwLock as ParkingRwLock, RwLockWriteGuard as ParkingRwLockWriteGuard};
use reifydb_core::interface::{SingleVersionCommandTransaction, SingleVersionQueryTransaction};
use reifydb_store_transaction::{SingleVersionCommit, SingleVersionContains, SingleVersionGet};
use reifydb_type::{diagnostic::transaction::key_out_of_scope, error, util::hex};
use self_cell::self_cell;
use tracing::debug_span;

use super::*;

type WriteGuard<'a> = ParkingRwLockWriteGuard<'a, ()>;

// Safe self-referential struct that owns both the Arc and the write guard borrowing from it
self_cell! {
	pub struct KeyWriteLock {
		owner: Arc<ParkingRwLock<()>>,
		#[covariant]
		dependent: WriteGuard,
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
	fn check_key_allowed(&self, key: &EncodedKey) -> crate::Result<()> {
		if self.keys.iter().any(|k| k == key) {
			Ok(())
		} else {
			Err(error!(key_out_of_scope(hex::encode(&key))))
		}
	}
}

impl SingleVersionQueryTransaction for SvlCommandTransaction<'_> {
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
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
			};
		}

		let _span = debug_span!("svl_get_from_store").entered();
		let store = {
			let _lock_span = debug_span!("svl_acquire_store_read_lock").entered();
			self.inner.store.read().unwrap()
		};
		store.get(key)
	}

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
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

		// Then check storage
		let store = self.inner.store.read().unwrap();
		store.contains(key)
	}
}

impl<'a> SingleVersionCommandTransaction for SvlCommandTransaction<'a> {
	fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> crate::Result<()> {
		self.check_key_allowed(key)?;

		let delta = Delta::Set {
			key: key.clone(),
			values,
		};
		self.pending.insert(key.clone(), delta);
		Ok(())
	}

	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.check_key_allowed(key)?;

		self.pending.insert(
			key.clone(),
			Delta::Remove {
				key: key.clone(),
			},
		);
		Ok(())
	}

	fn commit(mut self) -> crate::Result<()> {
		let _span = debug_span!("svl_commit").entered();

		let deltas: Vec<Delta> = take(&mut self.pending).into_iter().map(|(_, delta)| delta).collect();

		if !deltas.is_empty() {
			let mut store = {
				let _lock_span = debug_span!("svl_acquire_store_write_lock").entered();
				self.inner.store.write().unwrap()
			};
			{
				let _commit_span = debug_span!("svl_store_commit").entered();
				store.commit(CowVec::new(deltas))?;
			}
		}

		self.completed = true;
		Ok(())
	}

	fn rollback(mut self) -> crate::Result<()> {
		self.pending.clear();
		self.completed = true;
		Ok(())
	}
}
