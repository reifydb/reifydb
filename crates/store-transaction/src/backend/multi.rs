// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, TransactionId, delta::Delta, interface::MultiVersionValues,
};
use reifydb_type::Result;

use crate::{
	MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRange, MultiVersionRangeRev,
	MultiVersionScan, MultiVersionScanRev, MultiVersionStore,
	backend::{memory, sqlite},
	memory::MemoryBackend,
	sqlite::SqliteBackend,
};

#[repr(u8)]
#[derive(Clone)]
pub enum BackendMulti {
	Memory(MemoryBackend) = 0,
	Sqlite(SqliteBackend) = 1,
	// Custom(Box<dyn >) = 254, // High discriminant for future built-in backends
}

impl MultiVersionCommit for BackendMulti {
	#[inline(always)]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion, transaction: TransactionId) -> Result<()> {
		match self {
			BackendMulti::Memory(backend) => backend.commit(deltas, version, transaction),
			BackendMulti::Sqlite(backend) => backend.commit(deltas, version, transaction),
		}
	}
}

impl MultiVersionGet for BackendMulti {
	#[inline(always)]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionValues>> {
		match self {
			BackendMulti::Memory(backend) => backend.get(key, version),
			BackendMulti::Sqlite(backend) => backend.get(key, version),
		}
	}
}

impl MultiVersionContains for BackendMulti {
	#[inline(always)]
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
	type Item = MultiVersionValues;

	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendMultiScanIter::Memory(iter) => iter.next(),
			BackendMultiScanIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl MultiVersionScan for BackendMulti {
	type ScanIter<'a> = BackendMultiScanIter<'a>;

	#[inline(always)]
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
	type Item = MultiVersionValues;

	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendMultiScanIterRev::Memory(iter) => iter.next(),
			BackendMultiScanIterRev::Sqlite(iter) => iter.next(),
		}
	}
}

impl MultiVersionScanRev for BackendMulti {
	type ScanIterRev<'a> = BackendMultiScanIterRev<'a>;

	#[inline(always)]
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
	type Item = MultiVersionValues;

	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendMultiRangeIter::Memory(iter) => iter.next(),
			BackendMultiRangeIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl MultiVersionRange for BackendMulti {
	type RangeIter<'a> = BackendMultiRangeIter<'a>;

	#[inline(always)]
	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIter<'_>> {
		match self {
			BackendMulti::Memory(backend) => {
				backend.range(range, version).map(BackendMultiRangeIter::Memory)
			}
			BackendMulti::Sqlite(backend) => {
				backend.range(range, version).map(BackendMultiRangeIter::Sqlite)
			}
		}
	}
}

pub enum BackendMultiRangeIterRev<'a> {
	Memory(memory::MultiVersionRangeRevIter<'a>),
	Sqlite(sqlite::MultiVersionRangeRevIter),
}

impl<'a> Iterator for BackendMultiRangeIterRev<'a> {
	type Item = MultiVersionValues;

	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendMultiRangeIterRev::Memory(iter) => iter.next(),
			BackendMultiRangeIterRev::Sqlite(iter) => iter.next(),
		}
	}
}

impl MultiVersionRangeRev for BackendMulti {
	type RangeIterRev<'a> = BackendMultiRangeIterRev<'a>;

	#[inline(always)]
	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		match self {
			BackendMulti::Memory(backend) => {
				backend.range_rev(range, version).map(BackendMultiRangeIterRev::Memory)
			}
			BackendMulti::Sqlite(backend) => {
				backend.range_rev(range, version).map(BackendMultiRangeIterRev::Sqlite)
			}
		}
	}
}

impl MultiVersionStore for BackendMulti {}
