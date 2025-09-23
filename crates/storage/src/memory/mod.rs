// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{ops::Deref, sync::Arc};

pub use range::Range;
pub use range_rev::RangeRev;
pub use scan::MultiVersionIter;
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
	CommitVersion, EncodedKey,
	interface::{MultiVersionStorage, SingleVersionInsert, SingleVersionRemove, SingleVersionStorage},
	util::MultiVersionContainer,
	value::row::EncodedRow,
};

use crate::cdc::CdcTransaction;

pub type MultiVersionRowContainer = MultiVersionContainer<EncodedRow>;

#[derive(Clone)]
pub struct Memory(Arc<MemoryInner>);

pub struct MemoryInner {
	multi: SkipMap<EncodedKey, MultiVersionRowContainer>,
	single: SkipMap<EncodedKey, EncodedRow>,
	cdc_transactions: SkipMap<CommitVersion, CdcTransaction>,
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
			multi: SkipMap::new(),
			single: SkipMap::new(),
			cdc_transactions: SkipMap::new(),
		}))
	}
}

impl MultiVersionStorage for Memory {}
impl SingleVersionStorage for Memory {}
impl SingleVersionInsert for Memory {}
impl SingleVersionRemove for Memory {}
