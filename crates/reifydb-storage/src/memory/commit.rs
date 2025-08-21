// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, Result, Version,
	delta::Delta,
	interface::{CdcEventKey, UnversionedCommit, VersionedCommit},
	result::error::diagnostic::sequence,
	return_error,
	row::EncodedRow,
	util::now_millis,
};

use crate::{
	cdc::generate_cdc_event,
	memory::{Memory, versioned::VersionedRow},
};

impl VersionedCommit for Memory {
	fn commit(&self, delta: CowVec<Delta>, version: Version) -> Result<()> {
		let timestamp = now_millis();

		for (idx, delta) in delta.iter().enumerate() {
			let sequence = match u16::try_from(idx + 1) {
                Ok(seq) => seq,
                Err(_) => return_error!(sequence::transaction_sequence_exhausted()),
            };

			let before_value = self
				.versioned
				.get(delta.key())
				.and_then(|entry| {
					let values = entry.value();
					values.back().map(|e| {
						e.value()
							.clone()
							.unwrap_or_else(|| {
								EncodedRow::deleted()
							})
					})
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
					val.lock();
					val.insert(version, Some(row.clone()));
					val.unlock();
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

			// Generate and store CDC event
			let cdc_event = generate_cdc_event(
				delta.clone(),
				version,
				sequence,
				timestamp,
				before_value,
			);
			let cdc_key = CdcEventKey {
				version,
				sequence,
			};
			self.cdc_events.insert(cdc_key, cdc_event);
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
