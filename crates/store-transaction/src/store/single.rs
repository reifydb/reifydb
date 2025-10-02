// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::SingleVersionValues};

use super::{StandardTransactionStore, single_iterator::SingleVersionMergingIterator};
use crate::{
	SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionIter, SingleVersionRange,
	SingleVersionRangeRev, SingleVersionRemove, SingleVersionScan, SingleVersionScanRev, SingleVersionSet,
	SingleVersionStore,
};

impl SingleVersionGet for StandardTransactionStore {
	fn get(&self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		if let Some(hot) = &self.hot {
			if let Some(value) = hot.single.get(key)? {
				return Ok(Some(value));
			}
		}

		if let Some(warm) = &self.warm {
			if let Some(value) = warm.single.get(key)? {
				return Ok(Some(value));
			}
		}

		if let Some(cold) = &self.cold {
			return cold.single.get(key);
		}

		Ok(None)
	}
}

impl SingleVersionContains for StandardTransactionStore {
	fn contains(&self, key: &EncodedKey) -> crate::Result<bool> {
		if let Some(hot) = &self.hot {
			if hot.single.contains(key)? {
				return Ok(true);
			}
		}

		if let Some(warm) = &self.warm {
			if warm.single.contains(key)? {
				return Ok(true);
			}
		}

		if let Some(cold) = &self.cold {
			return cold.single.contains(key);
		}

		Ok(false)
	}
}

impl SingleVersionCommit for StandardTransactionStore {
	fn commit(&mut self, deltas: CowVec<Delta>) -> crate::Result<()> {
		if let Some(hot) = &mut self.hot {
			return hot.single.commit(deltas);
		}

		if let Some(warm) = &mut self.warm {
			return warm.single.commit(deltas);
		}

		if let Some(cold) = &mut self.cold {
			return cold.single.commit(deltas);
		}

		Ok(())
	}
}

impl SingleVersionSet for StandardTransactionStore {}
impl SingleVersionRemove for StandardTransactionStore {}

impl SingleVersionScan for StandardTransactionStore {
	type ScanIter<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	fn scan(&self) -> crate::Result<Self::ScanIter<'_>> {
		let mut iters: Vec<Box<dyn SingleVersionIter + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.single.scan()?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.single.scan()?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.single.scan()?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(SingleVersionMergingIterator::new(iters)))
	}
}

impl SingleVersionScanRev for StandardTransactionStore {
	type ScanIterRev<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	fn scan_rev(&self) -> crate::Result<Self::ScanIterRev<'_>> {
		let mut iters: Vec<Box<dyn SingleVersionIter + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.single.scan_rev()?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.single.scan_rev()?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.single.scan_rev()?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(SingleVersionMergingIterator::new(iters)))
	}
}

impl SingleVersionRange for StandardTransactionStore {
	type Range<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> crate::Result<Self::Range<'_>> {
		let mut iters: Vec<Box<dyn SingleVersionIter + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.single.range(range.clone())?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.single.range(range.clone())?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.single.range(range)?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(SingleVersionMergingIterator::new(iters)))
	}
}

impl SingleVersionRangeRev for StandardTransactionStore {
	type RangeRev<'a>
		= Box<dyn SingleVersionIter + 'a>
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange) -> crate::Result<Self::RangeRev<'_>> {
		let mut iters: Vec<Box<dyn SingleVersionIter + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.single.range_rev(range.clone())?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.single.range_rev(range.clone())?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.single.range_rev(range)?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(SingleVersionMergingIterator::new(iters)))
	}
}

impl SingleVersionStore for StandardTransactionStore {}
