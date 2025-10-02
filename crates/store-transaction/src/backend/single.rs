// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, delta::Delta, interface::SingleVersionValues};

use crate::{
	SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange, SingleVersionRangeRev,
	SingleVersionRemove, SingleVersionScan, SingleVersionScanRev, SingleVersionSet, SingleVersionStore,
	backend::{memory, sqlite},
	memory::MemoryBackend,
	sqlite::SqliteBackend,
};

#[repr(u8)]
#[derive(Clone)]
pub enum BackendSingle {
	Memory(MemoryBackend) = 0,
	Sqlite(SqliteBackend) = 1,
	// Custom(Box<dyn >) = 254, // High discriminant for future built-in backends
}

impl SingleVersionCommit for BackendSingle {
	fn commit(&mut self, deltas: CowVec<Delta>) -> reifydb_type::Result<()> {
		match self {
			BackendSingle::Memory(backend) => backend.commit(deltas),
			BackendSingle::Sqlite(backend) => backend.commit(deltas),
		}
	}
}

impl SingleVersionGet for BackendSingle {
	fn get(&self, key: &EncodedKey) -> reifydb_type::Result<Option<SingleVersionValues>> {
		match self {
			BackendSingle::Memory(backend) => backend.get(key),
			BackendSingle::Sqlite(backend) => backend.get(key),
		}
	}
}

impl SingleVersionContains for BackendSingle {
	fn contains(&self, key: &EncodedKey) -> reifydb_type::Result<bool> {
		match self {
			BackendSingle::Memory(backend) => backend.contains(key),
			BackendSingle::Sqlite(backend) => backend.contains(key),
		}
	}
}

impl SingleVersionSet for BackendSingle {}

impl SingleVersionRemove for BackendSingle {}

pub enum BackendSingleScanIter<'a> {
	Memory(memory::SingleVersionScanIter<'a>),
	Sqlite(sqlite::SingleVersionScanIter),
}

impl<'a> Iterator for BackendSingleScanIter<'a> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendSingleScanIter::Memory(iter) => iter.next(),
			BackendSingleScanIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl SingleVersionScan for BackendSingle {
	type ScanIter<'a> = BackendSingleScanIter<'a>;

	fn scan(&self) -> reifydb_type::Result<Self::ScanIter<'_>> {
		match self {
			BackendSingle::Memory(backend) => backend.scan().map(BackendSingleScanIter::Memory),
			BackendSingle::Sqlite(backend) => backend.scan().map(BackendSingleScanIter::Sqlite),
		}
	}
}

pub enum BackendSingleScanIterRev<'a> {
	Memory(memory::SingleVersionScanRevIter<'a>),
	Sqlite(sqlite::SingleVersionScanRevIter),
}

impl<'a> Iterator for BackendSingleScanIterRev<'a> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendSingleScanIterRev::Memory(iter) => iter.next(),
			BackendSingleScanIterRev::Sqlite(iter) => iter.next(),
		}
	}
}

impl SingleVersionScanRev for BackendSingle {
	type ScanIterRev<'a> = BackendSingleScanIterRev<'a>;

	fn scan_rev(&self) -> reifydb_type::Result<Self::ScanIterRev<'_>> {
		match self {
			BackendSingle::Memory(backend) => backend.scan_rev().map(BackendSingleScanIterRev::Memory),
			BackendSingle::Sqlite(backend) => backend.scan_rev().map(BackendSingleScanIterRev::Sqlite),
		}
	}
}

pub enum BackendSingleRangeIter<'a> {
	Memory(memory::SingleVersionRangeIter<'a>),
	Sqlite(sqlite::SingleVersionRangeIter),
}

impl<'a> Iterator for BackendSingleRangeIter<'a> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendSingleRangeIter::Memory(iter) => iter.next(),
			BackendSingleRangeIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl SingleVersionRange for BackendSingle {
	type Range<'a> = BackendSingleRangeIter<'a>;

	fn range(&self, range: EncodedKeyRange) -> reifydb_type::Result<Self::Range<'_>> {
		match self {
			BackendSingle::Memory(backend) => backend.range(range).map(BackendSingleRangeIter::Memory),
			BackendSingle::Sqlite(backend) => backend.range(range).map(BackendSingleRangeIter::Sqlite),
		}
	}
}

pub enum BackendSingleRangeIterRev<'a> {
	Memory(memory::SingleVersionRangeRevIter<'a>),
	Sqlite(sqlite::SingleVersionRangeRevIter),
}

impl<'a> Iterator for BackendSingleRangeIterRev<'a> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendSingleRangeIterRev::Memory(iter) => iter.next(),
			BackendSingleRangeIterRev::Sqlite(iter) => iter.next(),
		}
	}
}

impl SingleVersionRangeRev for BackendSingle {
	type RangeRev<'a> = BackendSingleRangeIterRev<'a>;

	fn range_rev(&self, range: EncodedKeyRange) -> reifydb_type::Result<Self::RangeRev<'_>> {
		match self {
			BackendSingle::Memory(backend) => {
				backend.range_rev(range).map(BackendSingleRangeIterRev::Memory)
			}
			BackendSingle::Sqlite(backend) => {
				backend.range_rev(range).map(BackendSingleRangeIterRev::Sqlite)
			}
		}
	}
}

impl SingleVersionStore for BackendSingle {}
