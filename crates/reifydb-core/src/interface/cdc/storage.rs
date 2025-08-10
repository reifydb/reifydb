// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::CdcEvent;
use crate::{Result, Version};

/// Combined trait for all CDC storage operations
pub trait CdcStorage: CdcGet + CdcRange + CdcScan + CdcCount {}

/// Retrieve CDC events for a specific version
pub trait CdcGet: Send + Sync {
    fn get(&self, version: Version) -> Result<Vec<CdcEvent>>;
}

/// Query CDC events within a version range
pub trait CdcRange: Send + Sync {
    fn range(&self, start_version: Version, end_version: Version) -> Result<Vec<CdcEvent>>;
}

/// Scan all CDC events
pub trait CdcScan: Send + Sync {
    fn scan(&self) -> Result<Vec<CdcEvent>>;
}

/// Count CDC events for a specific version
pub trait CdcCount: Send + Sync {
    fn count(&self, version: Version) -> Result<usize>;
}
