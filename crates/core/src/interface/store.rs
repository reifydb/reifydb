// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::TransactionId,
	value::row::EncodedRow,
};

#[derive(Debug)]
pub struct MultiVersionRow {
	pub key: EncodedKey,
	pub row: EncodedRow,
	pub version: CommitVersion,
}

#[derive(Debug)]
pub struct SingleVersionRow {
	pub key: EncodedKey,
	pub row: EncodedRow,
}

pub trait MultiVersionStorage:
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
	fn commit(
		&self,
		deltas: CowVec<Delta>,
		version: CommitVersion,
		transaction: TransactionId,
	) -> crate::Result<()>;
}

pub trait MultiVersionGet {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionRow>>;
}

pub trait MultiVersionContains {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<bool>;
}

pub trait MultiVersionIter: Iterator<Item = MultiVersionRow> + Send {}
impl<T: Send> MultiVersionIter for T where T: Iterator<Item = MultiVersionRow> {}

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

pub trait SingleVersionStorage:
	Send
	+ Sync
	+ Clone
	+ SingleVersionCommit
	+ SingleVersionGet
	+ SingleVersionContains
	+ SingleVersionInsert
	+ SingleVersionRemove
	+ SingleVersionScan
	+ SingleVersionScanRev
	+ SingleVersionRange
	+ SingleVersionRangeRev
	+ 'static
{
}

pub trait SingleVersionCommit {
	fn commit(&mut self, deltas: CowVec<Delta>) -> crate::Result<()>;
}

pub trait SingleVersionGet {
	fn get(&self, key: &EncodedKey) -> crate::Result<Option<SingleVersionRow>>;
}

pub trait SingleVersionContains {
	fn contains(&self, key: &EncodedKey) -> crate::Result<bool>;
}

pub trait SingleVersionInsert: SingleVersionCommit {
	fn insert(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				row: row.clone(),
			}]),
		)
	}
}

pub trait SingleVersionRemove: SingleVersionCommit {
	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Remove {
				key: key.clone(),
			}]),
		)
	}
}

pub trait SingleVersionIter: Iterator<Item = SingleVersionRow> + Send {}
impl<T> SingleVersionIter for T where T: Iterator<Item = SingleVersionRow> + Send {}

pub trait SingleVersionScan {
	type ScanIter<'a>: SingleVersionIter
	where
		Self: 'a;

	fn scan(&self) -> crate::Result<Self::ScanIter<'_>>;
}

pub trait SingleVersionScanRev {
	type ScanIterRev<'a>: SingleVersionIter
	where
		Self: 'a;

	fn scan_rev(&self) -> crate::Result<Self::ScanIterRev<'_>>;
}

pub trait SingleVersionRange {
	type Range<'a>: SingleVersionIter
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> crate::Result<Self::Range<'_>>;

	fn prefix(&self, prefix: &EncodedKey) -> crate::Result<Self::Range<'_>> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionRangeRev {
	type RangeRev<'a>: SingleVersionIter
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange) -> crate::Result<Self::RangeRev<'_>>;

	fn prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<Self::RangeRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}

/// Trait for row stores supporting columnar migration
pub trait RowStore: Send + Sync + Clone + 'static {
	/// Get the last version that was merged to column store
	fn last_merge_version(&self) -> CommitVersion;

	/// Count of rows pending merge
	fn pending_row_count(&self) -> usize;

	/// Check if merge should be triggered
	fn should_merge(&self) -> bool;

	/// Get batch of rows for merging
	fn get_merge_batch(&self, limit: usize) -> crate::Result<Vec<MultiVersionRow>>;

	/// Mark rows as merged and evict them from row store
	/// This is called after successful column store write
	fn mark_merged_and_evict(&self, up_to_version: CommitVersion) -> crate::Result<usize>;

	/// Verify data can be safely evicted (column store has it)
	fn verify_safe_to_evict(&self, up_to_version: CommitVersion) -> crate::Result<bool>;

	/// Get retention period for hot data
	fn retention_period(&self) -> std::time::Duration;
}
