// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::{
	collections::HashSet,
	sync::{LazyLock, RwLock},
};

use reifydb_core::{
	CowVec, Result, Version, delta::Delta, interface::VersionedCommit,
	result::error::diagnostic::sequence, return_error, row::EncodedRow,
};
use rusqlite::params;

use super::{ensure_table_exists, table_name};
use crate::{
	cdc::generate_cdc_event,
	sqlite::{
		Sqlite,
		cdc::{fetch_before_value, store_cdc_event},
	},
};

static ENSURED_TABLES: LazyLock<RwLock<HashSet<String>>> =
	LazyLock::new(|| RwLock::new(HashSet::new()));

impl VersionedCommit for Sqlite {
	fn commit(&self, delta: CowVec<Delta>, version: Version) -> Result<()> {
		let mut conn = self.get_conn();
		let tx = conn.transaction().unwrap();

		let timestamp = self.clock.now_millis();

		for (idx, delta) in delta.iter().enumerate() {
			let sequence = match u16::try_from(idx + 1) {
                Ok(seq) => seq,
                Err(_) => return_error!(sequence::transaction_sequence_exhausted()),
            };

			let table = table_name(delta.key());
			let before_value =
				fetch_before_value(&tx, delta.key(), table)
					.ok()
					.flatten();

			// Apply the data change
			match &delta {
				Delta::Set {
					key,
					row,
				} => {
					let table = table_name(&key);

					if table != "versioned" {
						let ensured_tables =
							ENSURED_TABLES
								.read()
								.unwrap();
						if !ensured_tables
							.contains(table)
						{
							drop(ensured_tables);
							let mut ensured_tables =
								ENSURED_TABLES
									.write()
									.unwrap(
									);
							if !ensured_tables
								.contains(table)
							{
								ensure_table_exists(&tx, &table);
								ensured_tables
									.insert(table
									.to_string(
									));
							}
						}
					}

					let query = format!(
						"INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)",
						table
					);
					tx.execute(
						&query,
						params![
							key.to_vec(),
							version,
							row.to_vec()
						],
					)
					.unwrap();
				}
				Delta::Remove {
					key,
				} => {
					let table = table_name(&key);
					let query = format!(
						"INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)",
						table
					);
					tx.execute(
						&query,
						params![
							key.to_vec(),
							version,
							EncodedRow::deleted()
								.to_vec()
						],
					)
					.unwrap();
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
			store_cdc_event(&tx, cdc_event, version, sequence)
				.unwrap();
		}

		tx.commit().unwrap();
		Ok(())
	}
}
