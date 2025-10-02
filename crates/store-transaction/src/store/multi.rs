// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, TransactionId, delta::Delta, interface::MultiVersionValues,
};

use super::StandardTransactionStore;
use crate::{
	MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionIter, MultiVersionRange,
	MultiVersionRangeRev, MultiVersionScan, MultiVersionScanRev, MultiVersionStore,
	store::multi_iterator::MultiVersionMergingIterator,
};

impl MultiVersionGet for StandardTransactionStore {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>> {
		if let Some(hot) = &self.hot {
			if let Some(values) = hot.multi.get(key, version)? {
				return Ok(Some(values));
			}
		}

		if let Some(warm) = &self.warm {
			if let Some(values) = warm.multi.get(key, version)? {
				return Ok(Some(values));
			}
		}

		if let Some(cold) = &self.cold {
			return cold.multi.get(key, version);
		}

		Ok(None)
	}
}

impl MultiVersionContains for StandardTransactionStore {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<bool> {
		Ok(MultiVersionGet::get(self, key, version)?.is_some())
	}
}

impl MultiVersionCommit for StandardTransactionStore {
	fn commit(
		&self,
		deltas: CowVec<Delta>,
		version: CommitVersion,
		transaction: TransactionId,
	) -> crate::Result<()> {
		if let Some(hot) = &self.hot {
			return hot.multi.commit(deltas, version, transaction);
		}

		if let Some(warm) = &self.warm {
			return warm.multi.commit(deltas, version, transaction);
		}

		if let Some(cold) = &self.cold {
			return cold.multi.commit(deltas, version, transaction);
		}

		Ok(())
	}
}

impl MultiVersionScan for StandardTransactionStore {
	type ScanIter<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn scan(&self, version: CommitVersion) -> crate::Result<Self::ScanIter<'_>> {
		let mut iters: Vec<Box<dyn MultiVersionIter + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.multi.scan(version)?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.multi.scan(version)?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.multi.scan(version)?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(MultiVersionMergingIterator::new(iters)))
	}
}

impl MultiVersionScanRev for StandardTransactionStore {
	type ScanIterRev<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn scan_rev(&self, version: CommitVersion) -> crate::Result<Self::ScanIterRev<'_>> {
		let mut iters: Vec<Box<dyn MultiVersionIter + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.multi.scan_rev(version)?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.multi.scan_rev(version)?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.multi.scan_rev(version)?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(MultiVersionMergingIterator::new(iters)))
	}
}

impl MultiVersionRange for StandardTransactionStore {
	type RangeIter<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> crate::Result<Self::RangeIter<'_>> {
		let mut iters: Vec<Box<dyn MultiVersionIter + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.multi.range(range.clone(), version)?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.multi.range(range.clone(), version)?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.multi.range(range, version)?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(MultiVersionMergingIterator::new(iters)))
	}
}

impl MultiVersionRangeRev for StandardTransactionStore {
	type RangeIterRev<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> crate::Result<Self::RangeIterRev<'_>> {
		let mut iters: Vec<Box<dyn MultiVersionIter + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.multi.range_rev(range.clone(), version)?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.multi.range_rev(range.clone(), version)?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.multi.range_rev(range, version)?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(MultiVersionMergingIterator::new(iters)))
	}
}

impl MultiVersionStore for StandardTransactionStore {}
