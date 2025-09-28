// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, CowVec, Result,
	delta::Delta,
	interface::{Cdc, CdcSequencedChange, MultiVersionCommit, SingleVersionCommit, TransactionId},
	return_error,
	util::now_millis,
};

use crate::{
	cdc::generate_cdc_change,
	diagnostic::sequence_exhausted,
	memory::{Memory, MultiVersionRowContainer},
};

impl MultiVersionCommit for Memory {
	fn commit(&self, delta: CowVec<Delta>, version: CommitVersion, transaction: TransactionId) -> Result<()> {
		let timestamp = now_millis();

		let mut cdcs = Vec::new();

		for (idx, delta) in delta.iter().enumerate() {
			let sequence = match u16::try_from(idx + 1) {
				Ok(seq) => seq,
				Err(_) => return_error!(sequence_exhausted()),
			};

			let before_value = self.multi.get(delta.key()).and_then(|entry| {
				let values = entry.value();
				values.get_latest()
			});

			match &delta {
				Delta::Set {
					key,
					row,
				} => {
					let item = self
						.multi
						.get_or_insert_with(key.clone(), MultiVersionRowContainer::new);
					let val = item.value();
					val.insert(version, Some(row.clone()));
				}
				Delta::Remove {
					key,
				} => {
					if let Some(values) = self.multi.get(key) {
						let values = values.value();
						if !values.is_empty() {
							values.insert(version, None);
						}
					}
				}
			}

			cdcs.push(CdcSequencedChange {
				sequence,
				change: generate_cdc_change(delta.clone(), before_value),
			});
		}

		if !cdcs.is_empty() {
			let cdc = Cdc::new(version, timestamp, transaction, cdcs);
			self.cdcs.insert(version, cdc);
		}

		Ok(())
	}
}

impl SingleVersionCommit for Memory {
	fn commit(&mut self, delta: CowVec<Delta>) -> Result<()> {
		for delta in delta {
			match delta {
				Delta::Set {
					key,
					row,
				} => {
					self.single.insert(key, row);
				}
				Delta::Remove {
					key,
				} => {
					self.single.remove(&key);
				}
			}
		}
		Ok(())
	}
}
