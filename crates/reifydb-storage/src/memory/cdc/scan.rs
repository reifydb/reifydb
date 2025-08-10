// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::memory::Memory;
use reifydb_core::interface::{CdcEvent, CdcScan};
use reifydb_core::Result;

impl CdcScan for Memory {
    fn scan(&self) -> Result<Vec<CdcEvent>> {
        let events: Vec<CdcEvent> = self.cdc_events
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        
        Ok(events)
    }
}