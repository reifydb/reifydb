// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::memory::Memory;
use reifydb_core::interface::{CdcEvent, CdcEventKey, CdcGet};
use reifydb_core::{Result, Version};

impl CdcGet for Memory {
    fn get(&self, version: Version) -> Result<Vec<CdcEvent>> {
        let start_key = CdcEventKey { version, sequence: 0 };
        let end_key = CdcEventKey { version: version + 1, sequence: 0 };
        
        let events: Vec<CdcEvent> = self.cdc_events
            .range(start_key..end_key)
            .map(|entry| entry.value().clone())
            .collect();
        
        Ok(events)
    }
}