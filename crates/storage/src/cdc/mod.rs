// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod codec;
mod layout;

use reifydb_core::{
	CommitVersion,
	delta::Delta,
	interface::{CdcChange, CdcEvent, TransactionId},
	row::EncodedRow,
};

/// Internal structure for storing CDC data with shared metadata
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CdcTransaction {
	pub version: CommitVersion,
	pub timestamp: u64,
	pub transaction: TransactionId,
	pub changes: Vec<CdcTransactionChange>,
}

/// Internal structure for individual changes within a transaction
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CdcTransactionChange {
	pub sequence: u16,
	pub change: CdcChange,
}

impl CdcTransaction {
	pub fn new(
		version: CommitVersion,
		timestamp: u64,
		transaction: TransactionId,
		changes: Vec<CdcTransactionChange>,
	) -> Self {
		Self {
			version,
			timestamp,
			transaction,
			changes,
		}
	}

	/// Convert internal transaction format to public CdcEvent iterator
	pub fn to_events(&self) -> impl Iterator<Item = CdcEvent> + '_ {
		self.changes.iter().map(|change| {
			CdcEvent::new(
				self.version,
				change.sequence,
				self.timestamp,
				self.transaction,
				change.change.clone(),
			)
		})
	}
}

/// Generate a CDC change from a Delta
pub(crate) fn generate_cdc_change(delta: Delta, before_value: Option<EncodedRow>) -> CdcChange {
	match delta {
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
			before: before_value.unwrap_or_else(|| EncodedRow::deleted()),
		},
	}
}
