// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::CdcEvent;
use crate::{Result, Version};
use std::ops::Bound;

/// Combined trait for all CDC storage operations
pub trait CdcQuery: CdcGet + CdcRange + CdcScan + CdcCount {}

/// Retrieve CDC events for a specific version
pub trait CdcGet: Send + Sync {
    fn get(&self, version: Version) -> Result<Vec<CdcEvent>>;
}

/// Query CDC events within a version range
pub trait CdcRange: Send + Sync {
    type RangeIter<'a>: Iterator<Item = CdcEvent> + 'a
    where
        Self: 'a;
    
    fn range(&self, start: Bound<Version>, end: Bound<Version>) -> Result<Self::RangeIter<'_>>;
}

/// Scan all CDC events
pub trait CdcScan: Send + Sync {
    type ScanIter<'a>: Iterator<Item = CdcEvent> + 'a
    where
        Self: 'a;
        
    fn scan(&self) -> Result<Self::ScanIter<'_>>;
}

/// Count CDC events for a specific version
pub trait CdcCount: Send + Sync {
    fn count(&self, version: Version) -> Result<usize>;
}
