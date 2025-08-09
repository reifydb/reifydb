// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
mod layout;
pub(crate) mod sequence;

use reifydb_core::Version;
use reifydb_core::delta::Delta;
use reifydb_core::interface::{CdcEvent, Change};
use reifydb_core::row::EncodedRow;

/// Generate a CDC event from a Delta change
pub(crate) fn generate_cdc_event(
    delta: &Delta,
    version: Version,
    sequence: u16,
    timestamp: u64,
    before_value: Option<EncodedRow>,
) -> CdcEvent {
    let change = match delta {
        Delta::Insert { key, row } => Change::Insert { key: key.clone(), after: row.clone() },
        Delta::Update { key, row } => Change::Update {
            key: key.clone(),
            before: before_value.unwrap_or_else(|| EncodedRow::deleted()),
            after: row.clone(),
        },
        Delta::Remove { key } => Change::Delete {
            key: key.clone(),
            before: before_value.unwrap_or_else(|| EncodedRow::deleted()),
        },
    };

    CdcEvent::new(version, sequence, timestamp, change)
}
