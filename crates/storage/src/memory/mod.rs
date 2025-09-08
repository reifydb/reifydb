// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{ops::Deref, sync::Arc};

pub use range::Range;
pub use range_rev::RangeRev;
pub use scan::VersionedIter;
pub use scan_rev::IterRev;

mod cdc;
mod commit;
mod contains;
mod get;
mod range;
mod range_rev;
mod scan;
mod scan_rev;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	EncodedKey, Version,
	interface::{
		UnversionedInsert, UnversionedRemove, UnversionedStorage,
		VersionedStorage,
	},
	row::EncodedRow,
	util::VersionedContainer,
};

use crate::cdc::CdcTransaction;

pub type VersionedRow = VersionedContainer<EncodedRow>;

#[derive(Clone)]
pub struct Memory(Arc<MemoryInner>);

pub struct MemoryInner {
	versioned: SkipMap<EncodedKey, VersionedRow>,
	unversioned: SkipMap<EncodedKey, EncodedRow>,
	cdc_transactions: SkipMap<Version, CdcTransaction>,
}

impl Deref for Memory {
	type Target = MemoryInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Default for Memory {
	fn default() -> Self {
		Self::new()
	}
}

impl Memory {
	pub fn new() -> Self {
		Self(Arc::new(MemoryInner {
			versioned: SkipMap::new(),
			unversioned: SkipMap::new(),
			cdc_transactions: SkipMap::new(),
		}))
	}
}

impl VersionedStorage for Memory {}
impl UnversionedStorage for Memory {}
impl UnversionedInsert for Memory {}
impl UnversionedRemove for Memory {}
