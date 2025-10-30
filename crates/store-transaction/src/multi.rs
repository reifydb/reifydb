// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::MultiVersionValues};

pub trait MultiVersionStore:
	Send
	+ Sync
	+ Clone
	+ MultiVersionCommit
	+ MultiVersionGet
	+ MultiVersionContains
	+ MultiVersionScan
	+ MultiVersionScanRev
	+ MultiVersionRange
	+ MultiVersionRangeRev
	+ 'static
{
}

pub trait MultiVersionCommit {
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> crate::Result<()>;
}

pub trait MultiVersionGet {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>>;
}

pub trait MultiVersionContains {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<bool>;
}

pub trait MultiVersionIter: Iterator<Item = MultiVersionValues> + Send {}
impl<T: Send> MultiVersionIter for T where T: Iterator<Item = MultiVersionValues> {}

pub trait MultiVersionScan {
	type ScanIter<'a>: MultiVersionIter
	where
		Self: 'a;

	fn scan(&self, version: CommitVersion) -> crate::Result<Self::ScanIter<'_>>;
}

pub trait MultiVersionScanRev {
	type ScanIterRev<'a>: MultiVersionIter
	where
		Self: 'a;

	fn scan_rev(&self, version: CommitVersion) -> crate::Result<Self::ScanIterRev<'_>>;
}

pub trait MultiVersionRange {
	type RangeIter<'a>: MultiVersionIter
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> crate::Result<Self::RangeIter<'_>>;

	fn prefix(&self, prefix: &EncodedKey, version: CommitVersion) -> crate::Result<Self::RangeIter<'_>> {
		self.range(EncodedKeyRange::prefix(prefix), version)
	}
}

pub trait MultiVersionRangeRev {
	type RangeIterRev<'a>: MultiVersionIter
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> crate::Result<Self::RangeIterRev<'_>>;

	fn prefix_rev(&self, prefix: &EncodedKey, version: CommitVersion) -> crate::Result<Self::RangeIterRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix), version)
	}
}
