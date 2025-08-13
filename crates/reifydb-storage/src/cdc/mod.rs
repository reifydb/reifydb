// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
mod layout;

use reifydb_core::{
	Version,
	delta::Delta,
	interface::{CdcEvent, Change},
	row::EncodedRow,
};

/// Generate a CDC event from a Delta change
pub(crate) fn generate_cdc_event(
	delta: Delta,
	version: Version,
	sequence: u16,
	timestamp: u64,
	before_value: Option<EncodedRow>,
) -> CdcEvent {
	let change = match delta {
		Delta::Set {
			key,
			row,
		} => {
			if let Some(before) = before_value {
				Change::Update {
					key,
					before,
					after: row,
				}
			} else {
				Change::Insert {
					key,
					after: row,
				}
			}
		}

		Delta::Remove {
			key,
		} => Change::Delete {
			key,
			before: before_value
				.unwrap_or_else(|| EncodedRow::deleted()),
		},
	};

	CdcEvent::new(version, sequence, timestamp, change)
}
