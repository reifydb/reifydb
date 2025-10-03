// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
pub use reifydb_type::Result;

pub mod backend;
pub(crate) mod cdc;
pub mod config;
mod multi;
mod single;
mod store;

use std::collections::Bound;

pub use config::{BackendConfig, MergeConfig, RetentionConfig, TransactionStoreConfig};
pub use multi::*;
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, TransactionId,
	delta::Delta,
	interface::{Cdc, CdcCount, CdcGet, CdcRange, CdcScan, CdcStore, MultiVersionValues, SingleVersionValues},
};
pub use single::*;
pub use store::StandardTransactionStore;

pub mod memory {
	pub use crate::backend::memory::MemoryBackend;
}
pub mod sqlite {
	pub use crate::backend::sqlite::{SqliteBackend, SqliteConfig};
}

pub struct TransactionStoreVersion;

impl HasVersion for TransactionStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "store-transaction".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Transaction storage for OLTP operations and recent data".to_string(),
			r#type: ComponentType::Module,
		}
	}
}

#[repr(u8)]
#[derive(Clone)]
pub enum TransactionStore {
	Standard(StandardTransactionStore) = 0,
	// Other(Box<dyn >) = 254,
}

// MultiVersion trait implementations
impl MultiVersionGet for TransactionStore {
	#[inline]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionValues>> {
		match self {
			TransactionStore::Standard(store) => MultiVersionGet::get(store, key, version),
		}
	}
}

impl MultiVersionContains for TransactionStore {
	#[inline]
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		match self {
			TransactionStore::Standard(store) => MultiVersionContains::contains(store, key, version),
		}
	}
}

impl MultiVersionCommit for TransactionStore {
	#[inline]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion, transaction: TransactionId) -> Result<()> {
		match self {
			TransactionStore::Standard(store) => store.commit(deltas, version, transaction),
		}
	}
}

impl MultiVersionScan for TransactionStore {
	type ScanIter<'a> = <StandardTransactionStore as MultiVersionScan>::ScanIter<'a>;

	#[inline]
	fn scan(&self, version: CommitVersion) -> Result<Self::ScanIter<'_>> {
		match self {
			TransactionStore::Standard(store) => MultiVersionScan::scan(store, version),
		}
	}
}

impl MultiVersionScanRev for TransactionStore {
	type ScanIterRev<'a> = <StandardTransactionStore as MultiVersionScanRev>::ScanIterRev<'a>;

	#[inline]
	fn scan_rev(&self, version: CommitVersion) -> Result<Self::ScanIterRev<'_>> {
		match self {
			TransactionStore::Standard(store) => MultiVersionScanRev::scan_rev(store, version),
		}
	}
}

impl MultiVersionRange for TransactionStore {
	type RangeIter<'a> = <StandardTransactionStore as MultiVersionRange>::RangeIter<'a>;

	#[inline]
	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIter<'_>> {
		match self {
			TransactionStore::Standard(store) => MultiVersionRange::range(store, range, version),
		}
	}
}

impl MultiVersionRangeRev for TransactionStore {
	type RangeIterRev<'a> = <StandardTransactionStore as MultiVersionRangeRev>::RangeIterRev<'a>;

	#[inline]
	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		match self {
			TransactionStore::Standard(store) => MultiVersionRangeRev::range_rev(store, range, version),
		}
	}
}

// SingleVersion trait implementations
impl SingleVersionGet for TransactionStore {
	#[inline]
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionValues>> {
		match self {
			TransactionStore::Standard(store) => SingleVersionGet::get(store, key),
		}
	}
}

impl SingleVersionContains for TransactionStore {
	#[inline]
	fn contains(&self, key: &EncodedKey) -> Result<bool> {
		match self {
			TransactionStore::Standard(store) => SingleVersionContains::contains(store, key),
		}
	}
}

impl SingleVersionSet for TransactionStore {}

impl SingleVersionRemove for TransactionStore {}

impl SingleVersionCommit for TransactionStore {
	#[inline]
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()> {
		match self {
			TransactionStore::Standard(store) => SingleVersionCommit::commit(store, deltas),
		}
	}
}

impl SingleVersionScan for TransactionStore {
	type ScanIter<'a> = <StandardTransactionStore as SingleVersionScan>::ScanIter<'a>;

	#[inline]
	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		match self {
			TransactionStore::Standard(store) => SingleVersionScan::scan(store),
		}
	}
}

impl SingleVersionScanRev for TransactionStore {
	type ScanIterRev<'a> = <StandardTransactionStore as SingleVersionScanRev>::ScanIterRev<'a>;

	#[inline]
	fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>> {
		match self {
			TransactionStore::Standard(store) => SingleVersionScanRev::scan_rev(store),
		}
	}
}

impl SingleVersionRange for TransactionStore {
	type Range<'a> = <StandardTransactionStore as SingleVersionRange>::Range<'a>;

	#[inline]
	fn range(&self, range: EncodedKeyRange) -> Result<Self::Range<'_>> {
		match self {
			TransactionStore::Standard(store) => SingleVersionRange::range(store, range),
		}
	}
}

impl SingleVersionRangeRev for TransactionStore {
	type RangeRev<'a> = <StandardTransactionStore as SingleVersionRangeRev>::RangeRev<'a>;

	#[inline]
	fn range_rev(&self, range: EncodedKeyRange) -> Result<Self::RangeRev<'_>> {
		match self {
			TransactionStore::Standard(store) => SingleVersionRangeRev::range_rev(store, range),
		}
	}
}

// CDC trait implementations
impl CdcGet for TransactionStore {
	#[inline]
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
		match self {
			TransactionStore::Standard(store) => CdcGet::get(store, version),
		}
	}
}

impl CdcRange for TransactionStore {
	type RangeIter<'a> = <StandardTransactionStore as CdcRange>::RangeIter<'a>;

	#[inline]
	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Result<Self::RangeIter<'_>> {
		match self {
			TransactionStore::Standard(store) => CdcRange::range(store, start, end),
		}
	}
}

impl CdcScan for TransactionStore {
	type ScanIter<'a> = <StandardTransactionStore as CdcScan>::ScanIter<'a>;

	#[inline]
	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		match self {
			TransactionStore::Standard(store) => CdcScan::scan(store),
		}
	}
}

impl CdcCount for TransactionStore {
	#[inline]
	fn count(&self, version: CommitVersion) -> Result<usize> {
		match self {
			TransactionStore::Standard(store) => CdcCount::count(store, version),
		}
	}
}

// High-level trait implementations
impl MultiVersionStore for TransactionStore {}
impl SingleVersionStore for TransactionStore {}
impl CdcStore for TransactionStore {}
