// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, CowVec, EncodedKey, EncodedKeyRange, delta::Delta};
use reifydb_type::Result;

use crate::{
	backend::{
		memory,
		result::{MultiVersionGetResult, MultiVersionIterResult},
		sqlite,
	},
	memory::MemoryBackend,
	sqlite::SqliteBackend,
};

#[repr(u8)]
#[derive(Clone)]
pub enum BackendMulti {
	Memory(MemoryBackend) = 0,
	Sqlite(SqliteBackend) = 1,
	// Other(Box<dyn >) = 254,
}

impl BackendMultiVersionCommit for BackendMulti {
	#[inline]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()> {
		match self {
			BackendMulti::Memory(backend) => backend.commit(deltas, version),
			BackendMulti::Sqlite(backend) => backend.commit(deltas, version),
		}
	}
}

impl BackendMultiVersionGet for BackendMulti {
	#[inline]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<MultiVersionGetResult> {
		match self {
			BackendMulti::Memory(backend) => backend.get(key, version),
			BackendMulti::Sqlite(backend) => backend.get(key, version),
		}
	}
}

impl BackendMultiVersionContains for BackendMulti {
	#[inline]
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		match self {
			BackendMulti::Memory(backend) => backend.contains(key, version),
			BackendMulti::Sqlite(backend) => backend.contains(key, version),
		}
	}
}

pub enum BackendMultiScanIter<'a> {
	Memory(memory::MultiVersionScanIter<'a>),
	Sqlite(sqlite::MultiVersionScanIter),
}

impl<'a> Iterator for BackendMultiScanIter<'a> {
	type Item = MultiVersionIterResult;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendMultiScanIter::Memory(iter) => iter.next(),
			BackendMultiScanIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl BackendMultiVersionScan for BackendMulti {
	type ScanIter<'a> = BackendMultiScanIter<'a>;

	#[inline]
	fn scan(&self, version: CommitVersion) -> Result<Self::ScanIter<'_>> {
		match self {
			BackendMulti::Memory(backend) => backend.scan(version).map(BackendMultiScanIter::Memory),
			BackendMulti::Sqlite(backend) => backend.scan(version).map(BackendMultiScanIter::Sqlite),
		}
	}
}

pub enum BackendMultiScanIterRev<'a> {
	Memory(memory::MultiVersionScanRevIter<'a>),
	Sqlite(sqlite::MultiVersionScanRevIter),
}

impl<'a> Iterator for BackendMultiScanIterRev<'a> {
	type Item = MultiVersionIterResult;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendMultiScanIterRev::Memory(iter) => iter.next(),
			BackendMultiScanIterRev::Sqlite(iter) => iter.next(),
		}
	}
}

impl BackendMultiVersionScanRev for BackendMulti {
	type ScanIterRev<'a> = BackendMultiScanIterRev<'a>;

	#[inline]
	fn scan_rev(&self, version: CommitVersion) -> Result<Self::ScanIterRev<'_>> {
		match self {
			BackendMulti::Memory(backend) => backend.scan_rev(version).map(BackendMultiScanIterRev::Memory),
			BackendMulti::Sqlite(backend) => backend.scan_rev(version).map(BackendMultiScanIterRev::Sqlite),
		}
	}
}

pub enum BackendMultiRangeIter<'a> {
	Memory(memory::MultiVersionRangeIter<'a>),
	Sqlite(sqlite::MultiVersionRangeIter),
}

impl<'a> Iterator for BackendMultiRangeIter<'a> {
	type Item = MultiVersionIterResult;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendMultiRangeIter::Memory(iter) => iter.next(),
			BackendMultiRangeIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl BackendMultiVersionRange for BackendMulti {
	type RangeIter<'a> = BackendMultiRangeIter<'a>;

	#[inline]
	fn range_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> Result<Self::RangeIter<'_>> {
		match self {
			BackendMulti::Memory(backend) => {
				backend.range_batched(range, version, batch_size).map(BackendMultiRangeIter::Memory)
			}
			BackendMulti::Sqlite(backend) => {
				backend.range_batched(range, version, batch_size).map(BackendMultiRangeIter::Sqlite)
			}
		}
	}
}

pub enum BackendMultiRangeIterRev<'a> {
	Memory(memory::MultiVersionRangeRevIter<'a>),
	Sqlite(sqlite::MultiVersionRangeRevIter),
}

impl<'a> Iterator for BackendMultiRangeIterRev<'a> {
	type Item = MultiVersionIterResult;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendMultiRangeIterRev::Memory(iter) => iter.next(),
			BackendMultiRangeIterRev::Sqlite(iter) => iter.next(),
		}
	}
}

impl BackendMultiVersionRangeRev for BackendMulti {
	type RangeIterRev<'a> = BackendMultiRangeIterRev<'a>;

	#[inline]
	fn range_rev_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> Result<Self::RangeIterRev<'_>> {
		match self {
			BackendMulti::Memory(backend) => backend
				.range_rev_batched(range, version, batch_size)
				.map(BackendMultiRangeIterRev::Memory),
			BackendMulti::Sqlite(backend) => backend
				.range_rev_batched(range, version, batch_size)
				.map(BackendMultiRangeIterRev::Sqlite),
		}
	}
}

impl BackendMultiVersion for BackendMulti {}

pub trait BackendMultiVersion:
	Send
	+ Sync
	+ Clone
	+ BackendMultiVersionCommit
	+ BackendMultiVersionGet
	+ BackendMultiVersionContains
	+ BackendMultiVersionScan
	+ BackendMultiVersionScanRev
	+ BackendMultiVersionRange
	+ BackendMultiVersionRangeRev
	+ 'static
{
}

pub trait BackendMultiVersionCommit {
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()>;
}

pub trait BackendMultiVersionGet {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<MultiVersionGetResult>;
}

pub trait BackendMultiVersionContains {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool>;
}

pub trait BackendMultiVersionIter: Iterator<Item = MultiVersionIterResult> + Send {}
impl<T: Send> BackendMultiVersionIter for T where T: Iterator<Item = MultiVersionIterResult> {}

pub trait BackendMultiVersionScan {
	type ScanIter<'a>: BackendMultiVersionIter
	where
		Self: 'a;

	fn scan(&self, version: CommitVersion) -> Result<Self::ScanIter<'_>>;
}

pub trait BackendMultiVersionScanRev {
	type ScanIterRev<'a>: BackendMultiVersionIter
	where
		Self: 'a;

	fn scan_rev(&self, version: CommitVersion) -> Result<Self::ScanIterRev<'_>>;
}

pub trait BackendMultiVersionRange {
	type RangeIter<'a>: BackendMultiVersionIter
	where
		Self: 'a;

	fn range_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> Result<Self::RangeIter<'_>>;

	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIter<'_>> {
		self.range_batched(range, version, 1024)
	}

	fn prefix(&self, prefix: &EncodedKey, version: CommitVersion) -> Result<Self::RangeIter<'_>> {
		self.range(EncodedKeyRange::prefix(prefix), version)
	}
}

pub trait BackendMultiVersionRangeRev {
	type RangeIterRev<'a>: BackendMultiVersionIter
	where
		Self: 'a;

	fn range_rev_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> Result<Self::RangeIterRev<'_>>;

	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		self.range_rev_batched(range, version, 1024)
	}

	fn prefix_rev(&self, prefix: &EncodedKey, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		self.range_rev(EncodedKeyRange::prefix(prefix), version)
	}
}
