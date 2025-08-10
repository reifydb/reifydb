// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::memory::Memory;
use reifydb_core::interface::{CdcEvent, CdcEventKey, CdcStorage};
use reifydb_core::{Result, Version};

impl CdcStorage for Memory {
    fn get_cdc_event(&self, version: Version, sequence: u16) -> Result<Option<CdcEvent>> {
        let key = CdcEventKey { version, sequence };
        Ok(self.cdc_events.get(&key).map(|entry| entry.value().clone()))
    }
    
    fn cdc_range(&self, start_version: Version, end_version: Version) -> Result<Vec<CdcEvent>> {
        let start_key = CdcEventKey { version: start_version, sequence: 0 };
        let end_key = CdcEventKey { version: end_version + 1, sequence: 0 };
        
        let events: Vec<CdcEvent> = self.cdc_events
            .range(start_key..end_key)
            .map(|entry| entry.value().clone())
            .collect();
        
        Ok(events)
    }
    
    fn cdc_scan(&self, limit: Option<usize>) -> Result<Vec<CdcEvent>> {
        let iter = self.cdc_events.iter().map(|entry| entry.value().clone());
        
        let events = if let Some(limit) = limit {
            iter.take(limit).collect()
        } else {
            iter.collect()
        };
        
        Ok(events)
    }
    
    fn cdc_count(&self, version: Version) -> Result<usize> {
        let start_key = CdcEventKey { version, sequence: 0 };
        let end_key = CdcEventKey { version: version + 1, sequence: 0 };
        
        let count = self.cdc_events
            .range(start_key..end_key)
            .count();
        
        Ok(count)
    }
    
    fn cdc_events_for_version(&self, version: Version) -> Result<Vec<CdcEvent>> {
        let start_key = CdcEventKey { version, sequence: 0 };
        let end_key = CdcEventKey { version: version + 1, sequence: 0 };
        
        let events: Vec<CdcEvent> = self.cdc_events
            .range(start_key..end_key)
            .map(|entry| entry.value().clone())
            .collect();
        
        Ok(events)
    }
}