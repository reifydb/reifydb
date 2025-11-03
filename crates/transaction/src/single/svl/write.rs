// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, mem::take, ops::RangeBounds, sync::RwLockWriteGuard};

use reifydb_core::interface::{BoxedSingleVersionIter, SingleVersionCommandTransaction, SingleVersionQueryTransaction};
use reifydb_store_transaction::{
	SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange, SingleVersionRangeRev,
};

use super::*;
use crate::single::svl::{range::SvlRangeIter, range_rev::SvlRangeRevIter};

pub struct SvlCommandTransaction<'a> {
	pending: HashMap<EncodedKey, Delta>,
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

	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter> {
		let (pending_items, committed_items) = self.prepare_range_data(range, false)?;
		let iter = SvlRangeIter::new(pending_items.into_iter(), committed_items.into_iter());
		Ok(Box::new(iter))
	}

	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter> {
		let (pending_items, committed_items) = self.prepare_range_data(range, true)?;
		let iter = SvlRangeRevIter::new(pending_items.into_iter(), committed_items.into_iter());
		Ok(Box::new(iter))
	}
}

impl<'a> SvlCommandTransaction<'a> {
	pub(super) fn new(store: RwLockWriteGuard<'a, TransactionStore>) -> Self {
		Self {
			pending: HashMap::new(),
			completed: false,
			store,
		}
	}

	/// Helper method to prepare range data by cloning and sorting pending
	/// items and collecting committed items from storage.
	fn prepare_range_data(
		&mut self,
		range: EncodedKeyRange,
		reverse: bool,
	) -> crate::Result<(Vec<(EncodedKey, Delta)>, Vec<SingleVersionValues>)> {
		// Clone and filter pending items from the buffer
		let mut pending_items: Vec<(EncodedKey, Delta)> = self
			.pending
			.iter()
			.filter(|(k, _)| range.contains(&**k))
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();

		// Sort pending items by key (forward or reverse)
		if reverse {
			pending_items.sort_by(|(l, _), (r, _)| r.cmp(l));
		} else {
			pending_items.sort_by(|(l, _), (r, _)| l.cmp(r));
		}

		// Get committed items from storage
		let committed_items: Vec<SingleVersionValues> = {
			if reverse {
				self.store.range_rev(range)?.collect()
			} else {
				self.store.range(range)?.collect()
			}
		};

		Ok((pending_items, committed_items))
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
