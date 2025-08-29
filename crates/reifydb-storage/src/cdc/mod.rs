// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
mod layout;

use reifydb_core::{
	Version,
	delta::Delta,
	interface::{CdcChange, CdcEvent, TransactionId},
	row::EncodedRow,
};

/// Generate a CDC event from a Delta change
pub(crate) fn generate_cdc_event(
	delta: Delta,
	version: Version,
	sequence: u16,
	timestamp: u64,
	transaction: TransactionId,
	before_value: Option<EncodedRow>,
) -> CdcEvent {
	let change = match delta {
		Delta::Set {
			key,
			row,
		} => {
			if let Some(before) = before_value {
				CdcChange::Update {
					key,
					before,
					after: row,
				}
			} else {
				CdcChange::Insert {
					key,
					after: row,
				}
			}
		}

		Delta::Remove {
			key,
		} => CdcChange::Delete {
			key,
			before: before_value
				.unwrap_or_else(|| EncodedRow::deleted()),
		},
	};

	CdcEvent::new(version, sequence, timestamp, transaction, change)
}
