// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Bound;

use reifydb_core::{
	encoded::key::EncodedKeyRange,
	interface::catalog::{column::ColumnDef, id::PrimaryKeyId, key::PrimaryKeyDef},
	key::{Key, primary_key::PrimaryKeyKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::primary_key::schema::{primary_key, primary_key::deserialize_column_ids},
};

pub struct PrimaryKeyInfo {
	pub def: PrimaryKeyDef,
	pub source_id: u64,
}

impl CatalogStore {
	pub(crate) fn list_primary_keys(rx: &mut Transaction<'_>) -> Result<Vec<PrimaryKeyInfo>> {
		let mut result = Vec::new();

		// Scan all primary key entries from storage
		// Note: Key encoding uses reverse order, so MAX encodes smaller
		// than 0
		let primary_key_range = {
			let start_key = PrimaryKeyKey::encoded(PrimaryKeyId(u64::MAX));
			let end_key = PrimaryKeyKey::encoded(PrimaryKeyId(0));

			EncodedKeyRange::new(Bound::Included(start_key), Bound::Included(end_key))
		};

		// Collect entries first to avoid borrow checker issues
		let mut entries = Vec::new();
		{
			let mut stream = rx.range(primary_key_range, 1024)?;
			while let Some(entry) = stream.next() {
				entries.push(entry?);
			}
		}

		for entry in entries {
			// Decode the primary key ID from the key
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::PrimaryKey(pk_key) = key {
					// Get the source ID from the primary
					// key record
					let source_id = primary_key::SCHEMA.get_u64(&entry.values, primary_key::SOURCE);

					// Deserialize column IDs
					let column_ids_blob =
						primary_key::SCHEMA.get_blob(&entry.values, primary_key::COLUMN_IDS);
					let column_ids = deserialize_column_ids(&column_ids_blob);

					// Fetch full ColumnDef for each column
					// ID
					let mut columns = Vec::new();
					for column_id in column_ids {
						let column_def = Self::get_column(rx, column_id)?;
						columns.push(ColumnDef {
							id: column_def.id,
							name: column_def.name,
							constraint: column_def.constraint,
							properties: column_def.properties,
							index: column_def.index,
							auto_increment: column_def.auto_increment,
							dictionary_id: None,
						});
					}

					let pk_def = PrimaryKeyDef {
						id: pk_key.primary_key,
						columns,
					};

					result.push(PrimaryKeyInfo {
						def: pk_def,
						source_id,
					});
				}
			}
		}

		Ok(result)
	}

	pub(crate) fn list_primary_key_columns(rx: &mut Transaction<'_>) -> Result<Vec<(u64, u64, usize)>> {
		let mut result = Vec::new();

		// Scan all primary key entries from storage using same approach
		// as list_primary_keys
		let primary_key_range = {
			let start_key = PrimaryKeyKey::encoded(PrimaryKeyId(u64::MAX));
			let end_key = PrimaryKeyKey::encoded(PrimaryKeyId(0));

			EncodedKeyRange::new(Bound::Included(start_key), Bound::Included(end_key))
		};

		let mut stream = rx.range(primary_key_range, 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
			// Decode the primary key ID from the key
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::PrimaryKey(pk_key) = key {
					// Deserialize column IDs from the
					// primary key record
					let column_ids_blob =
						primary_key::SCHEMA.get_blob(&entry.values, primary_key::COLUMN_IDS);
					let column_ids = deserialize_column_ids(&column_ids_blob);

					// Add each column with its position
					for (position, column_id) in column_ids.iter().enumerate() {
						result.push((
							pk_key.primary_key.0, // primary key id
							column_id.0,          // column id
							position,             // position in the primary key
						));
					}
				}
			}
		}

		Ok(result)
	}
}
