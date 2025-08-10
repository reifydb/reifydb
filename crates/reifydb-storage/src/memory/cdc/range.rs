// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::memory::Memory;
use reifydb_core::interface::{CdcEvent, CdcEventKey, CdcRange};
use reifydb_core::{Result, Version};
use std::ops::Bound;

impl CdcRange for Memory {
    fn range(&self, start: Bound<Version>, end: Bound<Version>) -> Result<Vec<CdcEvent>> {
        let start_key = match start {
            Bound::Included(v) => Bound::Included(CdcEventKey { version: v, sequence: 0 }),
            Bound::Excluded(v) => Bound::Excluded(CdcEventKey { version: v, sequence: u16::MAX }),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_key = match end {
            Bound::Included(v) => Bound::Included(CdcEventKey { version: v, sequence: u16::MAX }),
            Bound::Excluded(v) => {
                Bound::Excluded(CdcEventKey { version: v.saturating_sub(1), sequence: u16::MAX })
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let events: Vec<CdcEvent> = self
            .cdc_events
            .range((start_key, end_key))
            .map(|entry| entry.value().clone())
            .collect();

        Ok(events)
    }
}
