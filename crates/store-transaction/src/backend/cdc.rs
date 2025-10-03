// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use reifydb_core::{CommitVersion, interface::Cdc};

use crate::{
	CdcCount, CdcScan, CdcStore,
	backend::{memory, sqlite},
	cdc::{CdcGet, CdcRange},
	memory::MemoryBackend,
	sqlite::SqliteBackend,
};

#[repr(u8)]
#[derive(Clone)]
pub enum BackendCdc {
	Memory(MemoryBackend) = 0,
	Sqlite(SqliteBackend) = 1,
	// Other(Box<dyn >) = 254,
}

impl CdcGet for BackendCdc {
	#[inline]
	fn get(&self, version: CommitVersion) -> reifydb_type::Result<Option<Cdc>> {
		match self {
			BackendCdc::Memory(backend) => backend.get(version),
			BackendCdc::Sqlite(backend) => backend.get(version),
		}
	}
}

pub enum BackendCdcRangeIter<'a> {
	Memory(memory::CdcRangeIter<'a>),
	Sqlite(sqlite::CdcRangeIter),
}

impl<'a> Iterator for BackendCdcRangeIter<'a> {
	type Item = Cdc;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendCdcRangeIter::Memory(iter) => iter.next(),
			BackendCdcRangeIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl CdcRange for BackendCdc {
	type RangeIter<'a> = BackendCdcRangeIter<'a>;

	#[inline]
	fn range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> reifydb_type::Result<Self::RangeIter<'_>> {
		match self {
			BackendCdc::Memory(backend) => backend.range(start, end).map(BackendCdcRangeIter::Memory),
			BackendCdc::Sqlite(backend) => backend.range(start, end).map(BackendCdcRangeIter::Sqlite),
		}
	}
}

pub enum BackendCdcScanIter<'a> {
	Memory(memory::CdcScanIter<'a>),
	Sqlite(sqlite::CdcScanIter),
}

impl<'a> Iterator for BackendCdcScanIter<'a> {
	type Item = Cdc;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		match self {
			BackendCdcScanIter::Memory(iter) => iter.next(),
			BackendCdcScanIter::Sqlite(iter) => iter.next(),
		}
	}
}

impl CdcScan for BackendCdc {
	type ScanIter<'a> = BackendCdcScanIter<'a>;

	#[inline]
	fn scan(&self) -> reifydb_type::Result<Self::ScanIter<'_>> {
		match self {
			BackendCdc::Memory(backend) => backend.scan().map(BackendCdcScanIter::Memory),
			BackendCdc::Sqlite(backend) => backend.scan().map(BackendCdcScanIter::Sqlite),
		}
	}
}

impl CdcCount for BackendCdc {
	#[inline]
	fn count(&self, version: CommitVersion) -> reifydb_type::Result<usize> {
		match self {
			BackendCdc::Memory(backend) => backend.count(version),
			BackendCdc::Sqlite(backend) => backend.count(version),
		}
	}
}

impl CdcStore for BackendCdc {}
