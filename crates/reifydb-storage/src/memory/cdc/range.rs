// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::memory::Memory;
use reifydb_core::interface::{CdcEvent, CdcEventKey, CdcRange};
use reifydb_core::{Result, Version};

impl CdcRange for Memory {
    fn range(&self, start_version: Version, end_version: Version) -> Result<Vec<CdcEvent>> {
        let start_key = CdcEventKey { version: start_version, sequence: 0 };
        let end_key = CdcEventKey { version: end_version + 1, sequence: 0 };
        
        let events: Vec<CdcEvent> = self.cdc_events
            .range(start_key..end_key)
            .map(|entry| entry.value().clone())
            .collect();
        
        Ok(events)
    }
}