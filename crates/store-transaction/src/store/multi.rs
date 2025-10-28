// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::MultiVersionValues,
};

use super::StandardTransactionStore;
use crate::{
	MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionIter, MultiVersionRange,
	MultiVersionRangeRev, MultiVersionScan, MultiVersionScanRev, MultiVersionStore,
	backend::{
		multi::{
			BackendMultiVersionCommit, BackendMultiVersionGet, BackendMultiVersionRange,
			BackendMultiVersionRangeRev, BackendMultiVersionScan, BackendMultiVersionScanRev,
		},
		result::{MultiVersionGetResult, MultiVersionIterResult},
	},
	store::multi_iterator::MultiVersionMergingIterator,
};

impl MultiVersionGet for StandardTransactionStore {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>> {
		if let Some(hot) = &self.hot {
			match hot.multi.get(key, version)? {
				MultiVersionGetResult::Value(v) => return Ok(Some(v)),
				MultiVersionGetResult::Tombstone {
					..
				} => return Ok(None),
				MultiVersionGetResult::NotFound => {}
			}
		}

		if let Some(warm) = &self.warm {
			match warm.multi.get(key, version)? {
				MultiVersionGetResult::Value(v) => return Ok(Some(v)),
				MultiVersionGetResult::Tombstone {
					..
				} => return Ok(None),
				MultiVersionGetResult::NotFound => {}
			}
		}

		if let Some(cold) = &self.cold {
			return Ok(cold.multi.get(key, version)?.into());
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
	) -> crate::Result<()> {
		if let Some(hot) = &self.hot {
			return hot.multi.commit(deltas, version);
		}

		if let Some(warm) = &self.warm {
			return warm.multi.commit(deltas, version);
		}

		if let Some(cold) = &self.cold {
			return cold.multi.commit(deltas, version);
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
		let mut iters: Vec<Box<dyn Iterator<Item = MultiVersionIterResult> + Send + '_>> = Vec::new();

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
		let mut iters: Vec<Box<dyn Iterator<Item = MultiVersionIterResult> + Send + '_>> = Vec::new();

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
		let mut iters: Vec<Box<dyn Iterator<Item = MultiVersionIterResult> + Send + '_>> = Vec::new();

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
		let mut iters: Vec<Box<dyn Iterator<Item = MultiVersionIterResult> + Send + '_>> = Vec::new();

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
