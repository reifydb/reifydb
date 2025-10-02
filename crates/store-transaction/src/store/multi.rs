// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange,
	delta::Delta,
	interface::{
		MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRange, MultiVersionRangeRev,
		MultiVersionScan, MultiVersionScanRev, MultiVersionStore, MultiVersionValues, TransactionId,
	},
};

use super::StandardTransactionStore;

pub trait MultiVersionIter: Iterator<Item = MultiVersionValues> + Send {}
impl<T: Send> MultiVersionIter for T where T: Iterator<Item = MultiVersionValues> {}

impl MultiVersionGet for StandardTransactionStore {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>> {
		// Check hot tier first
		if let Some(hot) = &self.hot {
			if let Some(row) = hot.get(key, version)? {
				return Ok(Some(row));
			}
		}

		// Check warm tier
		if let Some(warm) = &self.warm {
			if let Some(row) = warm.get(key, version)? {
				return Ok(Some(row));
			}
		}

		// Check cold tier
		if let Some(cold) = &self.cold {
			return cold.get(key, version);
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
		_deltas: CowVec<Delta>,
		_version: CommitVersion,
		_transaction: TransactionId,
	) -> crate::Result<()> {
		todo!("Implement commit to hot tier")
	}
}

impl MultiVersionScan for StandardTransactionStore {
	type ScanIter<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn scan(&self, _version: CommitVersion) -> crate::Result<Self::ScanIter<'_>> {
		todo!("Implement scan across tiers")
	}
}

impl MultiVersionScanRev for StandardTransactionStore {
	type ScanIterRev<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn scan_rev(&self, _version: CommitVersion) -> crate::Result<Self::ScanIterRev<'_>> {
		todo!("Implement reverse scan across tiers")
	}
}

impl MultiVersionRange for StandardTransactionStore {
	type RangeIter<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn range(&self, _range: EncodedKeyRange, _version: CommitVersion) -> crate::Result<Self::RangeIter<'_>> {
		todo!("Implement range scan across tiers")
	}
}

impl MultiVersionRangeRev for StandardTransactionStore {
	type RangeIterRev<'a>
		= Box<dyn MultiVersionIter + 'a>
	where
		Self: 'a;

	fn range_rev(&self, _range: EncodedKeyRange, _version: CommitVersion) -> crate::Result<Self::RangeIterRev<'_>> {
		todo!("Implement reverse range scan across tiers")
	}
}

impl MultiVersionStore for StandardTransactionStore {}
