// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, mem::take, ops::RangeBounds, sync::RwLockWriteGuard};

use reifydb_core::interface::{BoxedSingleVersionIter, SingleVersionCommandTransaction, SingleVersionQueryTransaction};

use super::*;
use crate::single::svl::{
	range::SvlRangeIter, range_rev::SvlRangeRevIter, scan::SvlScanIter, scan_rev::SvlScanRevIter,
};

pub struct SvlCommandTransaction<'a, SVS> {
	pending: HashMap<EncodedKey, Delta>,
	completed: bool,
	storage: RwLockWriteGuard<'a, SVS>,
}

impl<SVS> SingleVersionQueryTransaction for SvlCommandTransaction<'_, SVS>
where
	SVS: SingleVersionStore,
{
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

		self.storage.get(key)
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
		self.storage.contains(key)
	}

	fn scan(&mut self) -> crate::Result<BoxedSingleVersionIter> {
		let (pending_items, committed_items) = self.prepare_scan_data(None, false)?;
		let iter = SvlScanIter::new(pending_items.into_iter(), committed_items.into_iter());
		Ok(Box::new(iter))
	}

	fn scan_rev(&mut self) -> crate::Result<BoxedSingleVersionIter> {
		let (pending_items, committed_items) = self.prepare_scan_data(None, true)?;
		let iter = SvlScanRevIter::new(pending_items.into_iter(), committed_items.into_iter());
		Ok(Box::new(iter))
	}

	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter> {
		let (pending_items, committed_items) = self.prepare_scan_data(Some(range.clone()), false)?;
		let iter = SvlRangeIter::new(pending_items.into_iter(), committed_items.into_iter());
		Ok(Box::new(iter))
	}

	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter> {
		let (pending_items, committed_items) = self.prepare_scan_data(Some(range.clone()), true)?;
		let iter = SvlRangeRevIter::new(pending_items.into_iter(), committed_items.into_iter());
		Ok(Box::new(iter))
	}
}

impl<'a, SVS> SvlCommandTransaction<'a, SVS>
where
	SVS: SingleVersionStore,
{
	pub(super) fn new(storage: RwLockWriteGuard<'a, SVS>) -> Self {
		Self {
			pending: HashMap::new(),
			completed: false,
			storage,
		}
	}

	/// Helper method to prepare scan data by cloning and sorting pending
	/// items and collecting committed items from storage.
	fn prepare_scan_data(
		&mut self,
		range: Option<EncodedKeyRange>,
		reverse: bool,
	) -> crate::Result<(Vec<(EncodedKey, Delta)>, Vec<SingleVersionValues>)> {
		// Clone and optionally filter pending items from the buffer
		let mut pending_items: Vec<(EncodedKey, Delta)> = match &range {
			Some(r) => self
				.pending
				.iter()
				.filter(|(k, _)| r.contains(&**k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect(),
			None => self.pending.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
		};

		// Sort pending items by key (forward or reverse)
		if reverse {
			pending_items.sort_by(|(l, _), (r, _)| r.cmp(l));
		} else {
			pending_items.sort_by(|(l, _), (r, _)| l.cmp(r));
		}

		// Get committed items from storage
		let committed_items: Vec<SingleVersionValues> = {
			match (range, reverse) {
				(Some(r), true) => self.storage.range_rev(r)?.collect(),
				(Some(r), false) => self.storage.range(r)?.collect(),
				(None, true) => self.storage.scan_rev()?.collect(),
				(None, false) => self.storage.scan()?.collect(),
			}
		};

		Ok((pending_items, committed_items))
	}
}

impl<'a, SVS> SingleVersionCommandTransaction for SvlCommandTransaction<'a, SVS>
where
	SVS: SingleVersionStore,
{
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
			self.storage.commit(CowVec::new(deltas))?;
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
