// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use scan::VersionedIter;
pub use scan_rev::IterRev;
pub use range::Range;
pub use range_rev::RangeRev;
use std::ops::Deref;
use std::sync::Arc;

mod apply;
mod contains;
mod get;
mod scan;
mod scan_rev;
mod range;
mod range_rev;
mod versioned;

use crate::memory::versioned::VersionedRow;
use crate::cdc::sequence::SequenceTracker;
use crossbeam_skiplist::SkipMap;
use reifydb_core::EncodedKey;
use reifydb_core::interface::{
	UnversionedRemove, UnversionedStorage, UnversionedInsert, VersionedStorage, CdcEvent, CdcEventKey,
};
use reifydb_core::row::EncodedRow;
use reifydb_core::util::{Clock, SystemClock};

#[derive(Clone)]
pub struct Memory(Arc<MemoryInner>);

pub struct MemoryInner {
    versioned: SkipMap<EncodedKey, VersionedRow>,
    unversioned: SkipMap<EncodedKey, EncodedRow>,
    cdc_events: SkipMap<CdcEventKey, CdcEvent>,
    cdc_seq: SequenceTracker,
    clock: Box<dyn Clock>,
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
        Self::with_clock(Box::new(SystemClock))
    }
    
    pub fn with_clock(clock: Box<dyn Clock>) -> Self {
        Self(Arc::new(MemoryInner { 
            versioned: SkipMap::new(), 
            unversioned: SkipMap::new(),
            cdc_events: SkipMap::new(),
            cdc_seq: SequenceTracker::new(),
            clock,
        }))
    }
}

impl VersionedStorage for Memory {}
impl UnversionedStorage for Memory {}
impl UnversionedInsert for Memory {}
impl UnversionedRemove for Memory {}
