// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{mem::take, sync::RwLockWriteGuard};

use indexmap::IndexMap;
use reifydb_core::interface::{SingleVersionCommandTransaction, SingleVersionQueryTransaction};
use reifydb_store_transaction::{SingleVersionCommit, SingleVersionContains, SingleVersionGet};

use super::*;

pub struct SvlCommandTransaction<'a> {
	pending: IndexMap<EncodedKey, Delta>,
	completed: bool,
	store: RwLockWriteGuard<'a, TransactionStore>,
}

impl SingleVersionQueryTransaction for SvlCommandTransaction<'_> {
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
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

		self.store.get(key)
	}

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
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
		self.store.contains(key)
	}
}

impl<'a> SvlCommandTransaction<'a> {
	pub(super) fn new(store: RwLockWriteGuard<'a, TransactionStore>) -> Self {
		Self {
			pending: IndexMap::new(),
			completed: false,
			store,
		}
	}
}

impl<'a> SingleVersionCommandTransaction for SvlCommandTransaction<'a> {
	fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> crate::Result<()> {
		let delta = Delta::Set {
			key: key.clone(),
			values,
		};
		self.pending.insert(key.clone(), delta);
		Ok(())
	}

	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.pending.insert(
			key.clone(),
			Delta::Remove {
				key: key.clone(),
			},
		);
		Ok(())
	}

	fn commit(mut self) -> crate::Result<()> {
		let deltas: Vec<Delta> = take(&mut self.pending).into_iter().map(|(_, delta)| delta).collect();

		if !deltas.is_empty() {
			self.store.commit(CowVec::new(deltas))?;
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
