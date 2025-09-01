// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, Result, Version,
	delta::Delta,
	interface::{TransactionId, UnversionedCommit, VersionedCommit},
	return_error,
	row::EncodedRow,
	util::now_millis,
};
use reifydb_type::diagnostic::sequence;

use crate::{
	cdc::{CdcTransaction, CdcTransactionChange, generate_cdc_change},
	memory::{Memory, VersionedRow},
};

impl VersionedCommit for Memory {
	fn commit(
		&self,
		delta: CowVec<Delta>,
		version: Version,
		transaction: TransactionId,
	) -> Result<()> {
		let timestamp = now_millis();

		let mut cdc_changes = Vec::new();

		for (idx, delta) in delta.iter().enumerate() {
			let sequence = match u16::try_from(idx + 1) {
                Ok(seq) => seq,
                Err(_) => return_error!(sequence::transaction_sequence_exhausted())};

			let before_value = self
				.versioned
				.get(delta.key())
				.and_then(|entry| {
					let values = entry.value();
					Some(values
						.get_latest()
						.unwrap_or_else(|| {
							EncodedRow::deleted()
						}))
				});

			match &delta {
				Delta::Set {
					key,
					row,
				} => {
					let item = self
						.versioned
						.get_or_insert_with(
							key.clone(),
							VersionedRow::new,
						);
					let val = item.value();
					val.insert(version, Some(row.clone()));
				}
				Delta::Remove {
					key,
				} => {
					if let Some(values) =
						self.versioned.get(key)
					{
						let values = values.value();
						if !values.is_empty() {
							values.insert(
								version, None,
							);
						}
					}
				}
			}

			cdc_changes.push(CdcTransactionChange {
				sequence,
				change: generate_cdc_change(
					delta.clone(),
					before_value,
				),
			});
		}

		if !cdc_changes.is_empty() {
			let cdc_transaction = CdcTransaction::new(
				version,
				timestamp,
				transaction,
				cdc_changes,
			);
			self.cdc_transactions.insert(version, cdc_transaction);
		}

		Ok(())
	}
}

impl UnversionedCommit for Memory {
	fn commit(&mut self, delta: CowVec<Delta>) -> Result<()> {
		for delta in delta {
			match delta {
				Delta::Set {
					key,
					row,
				} => {
					self.unversioned.insert(key, row);
				}
				Delta::Remove {
					key,
				} => {
					self.unversioned.remove(&key);
				}
			}
		}
		Ok(())
	}
}
