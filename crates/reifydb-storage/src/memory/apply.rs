// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::cdc::generate_cdc_event;
use crate::memory::Memory;
use crate::memory::versioned::VersionedRow;
use reifydb_core::delta::Delta;
use reifydb_core::interface::{UnversionedApply, VersionedApply, CdcEventKey};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, Result, Version};

impl VersionedApply for Memory {
    fn apply(&self, delta: CowVec<Delta>, version: Version) -> Result<()> {
        let timestamp = self.clock.now_millis();
        
        for delta in delta {
            let sequence = self.cdc_seq.next_sequence(version);
            
            // Get before value for updates and deletes
            let before_value = match &delta {
                Delta::Insert { .. } => None,
                Delta::Update { key, .. } | Delta::Remove { key } => {
                    // Get the current value before the change
                    self.versioned.get(key).and_then(|entry| {
                        let values = entry.value();
                        // Get the most recent value from the SkipMap
                        values.back().map(|e| {
                            e.value().clone().unwrap_or_else(|| EncodedRow::deleted())
                        })
                    })
                }
            };
            
            // Apply the data change
            match &delta {
                Delta::Insert { key, row } | Delta::Update { key, row } => {
                    let item = self.versioned.get_or_insert_with(key.clone(), VersionedRow::new);
                    let val = item.value();
                    val.lock();
                    val.insert(version, Some(row.clone()));
                    val.unlock();
                }
                Delta::Remove { key } => {
                    if let Some(values) = self.versioned.get(key) {
                        let values = values.value();
                        if !values.is_empty() {
                            values.insert(version, None);
                        }
                    }
                }
            }
            
            // Generate and store CDC event
            let cdc_event = generate_cdc_event(&delta, version, sequence, timestamp, before_value);
            let cdc_key = CdcEventKey { version, sequence };
            self.cdc_events.insert(cdc_key, cdc_event);
        }
        Ok(())
    }
}

impl UnversionedApply for Memory {
    fn apply(&mut self, delta: CowVec<Delta>) -> Result<()> {
        for delta in delta {
            match delta {
                Delta::Insert { key, row } | Delta::Update { key, row } => {
                    self.unversioned.insert(key, row);
                }
                Delta::Remove { key } => {
                    self.unversioned.remove(&key);
                }
            }
        }
        Ok(())
    }
}
