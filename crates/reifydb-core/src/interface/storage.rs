// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	CowVec, EncodedKey, EncodedKeyRange, Version, delta::Delta,
	interface::TransactionId, row::EncodedRow,
};

#[derive(Debug)]
pub struct Versioned {
	pub key: EncodedKey,
	pub row: EncodedRow,
	pub version: Version,
}

#[derive(Debug)]
pub struct Unversioned {
	pub key: EncodedKey,
	pub row: EncodedRow,
}

pub trait VersionedStorage:
	Send
	+ Sync
	+ Clone
	+ VersionedCommit
	+ VersionedGet
	+ VersionedContains
	+ VersionedScan
	+ VersionedScanRev
	+ VersionedRange
	+ VersionedRangeRev
	+ 'static
{
}

pub trait VersionedCommit {
	fn commit(
		&self,
		deltas: CowVec<Delta>,
		version: Version,
		transaction: TransactionId,
	) -> crate::Result<()>;
}

pub trait VersionedGet {
	fn get(
		&self,
		key: &EncodedKey,
		version: Version,
	) -> crate::Result<Option<Versioned>>;
}

pub trait VersionedContains {
	fn contains(
		&self,
		key: &EncodedKey,
		version: Version,
	) -> crate::Result<bool>;
}

pub trait VersionedIter: Iterator<Item = Versioned> + Send {}
impl<T: Send> VersionedIter for T where T: Iterator<Item = Versioned> {}

pub trait VersionedScan {
	type ScanIter<'a>: VersionedIter
	where
		Self: 'a;

	fn scan(&self, version: Version) -> crate::Result<Self::ScanIter<'_>>;
}

pub trait VersionedScanRev {
	type ScanIterRev<'a>: VersionedIter
	where
		Self: 'a;

	fn scan_rev(
		&self,
		version: Version,
	) -> crate::Result<Self::ScanIterRev<'_>>;
}

pub trait VersionedRange {
	type RangeIter<'a>: VersionedIter
	where
		Self: 'a;

	fn range(
		&self,
		range: EncodedKeyRange,
		version: Version,
	) -> crate::Result<Self::RangeIter<'_>>;

	fn prefix(
		&self,
		prefix: &EncodedKey,
		version: Version,
	) -> crate::Result<Self::RangeIter<'_>> {
		self.range(EncodedKeyRange::prefix(prefix), version)
	}
}

pub trait VersionedRangeRev {
	type RangeIterRev<'a>: VersionedIter
	where
		Self: 'a;

	fn range_rev(
		&self,
		range: EncodedKeyRange,
		version: Version,
	) -> crate::Result<Self::RangeIterRev<'_>>;

	fn prefix_rev(
		&self,
		prefix: &EncodedKey,
		version: Version,
	) -> crate::Result<Self::RangeIterRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix), version)
	}
}

pub trait UnversionedStorage:
	Send
	+ Sync
	+ Clone
	+ UnversionedCommit
	+ UnversionedGet
	+ UnversionedContains
	+ UnversionedInsert
	+ UnversionedRemove
	+ UnversionedScan
	+ UnversionedScanRev
	+ UnversionedRange
	+ UnversionedRangeRev
	+ 'static
{
}

pub trait UnversionedCommit {
	fn commit(&mut self, deltas: CowVec<Delta>) -> crate::Result<()>;
}

pub trait UnversionedGet {
	fn get(&self, key: &EncodedKey) -> crate::Result<Option<Unversioned>>;
}

pub trait UnversionedContains {
	fn contains(&self, key: &EncodedKey) -> crate::Result<bool>;
}

pub trait UnversionedInsert: UnversionedCommit {
	fn insert(
		&mut self,
		key: &EncodedKey,
		row: EncodedRow,
	) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				row: row.clone(),
			}]),
		)
	}
}

pub trait UnversionedRemove: UnversionedCommit {
	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Remove {
				key: key.clone(),
			}]),
		)
	}
}

pub trait UnversionedIter: Iterator<Item = Unversioned> + Send {}
impl<T> UnversionedIter for T where T: Iterator<Item = Unversioned> + Send {}

pub trait UnversionedScan {
	type ScanIter<'a>: UnversionedIter
	where
		Self: 'a;

	fn scan(&self) -> crate::Result<Self::ScanIter<'_>>;
}

pub trait UnversionedScanRev {
	type ScanIterRev<'a>: UnversionedIter
	where
		Self: 'a;

	fn scan_rev(&self) -> crate::Result<Self::ScanIterRev<'_>>;
}

pub trait UnversionedRange {
	type Range<'a>: UnversionedIter
	where
		Self: 'a;

	fn range(
		&self,
		range: EncodedKeyRange,
	) -> crate::Result<Self::Range<'_>>;

	fn prefix(
		&self,
		prefix: &EncodedKey,
	) -> crate::Result<Self::Range<'_>> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

pub trait UnversionedRangeRev {
	type RangeRev<'a>: UnversionedIter
	where
		Self: 'a;

	fn range_rev(
		&self,
		range: EncodedKeyRange,
	) -> crate::Result<Self::RangeRev<'_>>;

	fn prefix_rev(
		&self,
		prefix: &EncodedKey,
	) -> crate::Result<Self::RangeRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}
