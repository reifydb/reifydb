// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::CdcEvent;
use crate::{Result, Version};

/// Trait for CDC event storage and retrieval
pub trait CdcStorage: Send + Sync {
    /// Retrieve a specific CDC event by version and sequence
    fn get_cdc_event(&self, version: Version, sequence: u16) -> Result<Option<CdcEvent>>;

    /// Query CDC events within a version range
    fn cdc_range(&self, start_version: Version, end_version: Version) -> Result<Vec<CdcEvent>>;

    /// Scan all CDC events with optional limit
    fn cdc_scan(&self, limit: Option<usize>) -> Result<Vec<CdcEvent>>;

    /// Count CDC events for a specific version
    fn cdc_count(&self, version: Version) -> Result<usize>;

    /// Get all CDC events for a specific version
    fn cdc_events_for_version(&self, version: Version) -> Result<Vec<CdcEvent>>;
}
