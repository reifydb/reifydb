// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Bound;

use reifydb_core::{
	encoded::key::EncodedKeyRange,
	interface::catalog::{column::Column, id::PrimaryKeyId, key::PrimaryKey},
	key::{Key, primary_key::PrimaryKeyKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::primary_key::shape::{primary_key, primary_key::deserialize_column_ids},
};

pub struct PrimaryKeyInfo {
	pub def: PrimaryKey,
	pub shape_id: u64,
}

impl CatalogStore {
	pub(crate) fn list_primary_keys(rx: &mut Transaction<'_>) -> Result<Vec<PrimaryKeyInfo>> {
		let mut result = Vec::new();

		let primary_key_range = {
			let start_key = PrimaryKeyKey::encoded(PrimaryKeyId(u64::MAX));
			let end_key = PrimaryKeyKey::encoded(PrimaryKeyId(0));

			EncodedKeyRange::new(Bound::Included(start_key), Bound::Included(end_key))
		};

		let mut entries = Vec::new();
		{
			let stream = rx.range(primary_key_range, 1024)?;
			for entry in stream {
				entries.push(entry?);
			}
		}

		for entry in entries {
			if let Some(key) = Key::decode(&entry.key)
				&& let Key::PrimaryKey(pk_key) = key
			{
				let shape_id = primary_key::SHAPE.get_u64(&entry.row, primary_key::SOURCE);

				let column_ids_blob = primary_key::SHAPE.get_blob(&entry.row, primary_key::COLUMN_IDS);
				let column_ids = deserialize_column_ids(&column_ids_blob);

				let mut columns = Vec::new();
				for column_id in column_ids {
					let column = Self::get_column(rx, column_id)?;
					columns.push(Column {
						id: column.id,
						name: column.name,
						constraint: column.constraint,
						properties: column.properties,
						index: column.index,
						auto_increment: column.auto_increment,
						dictionary_id: None,
					});
				}

				let pk_def = PrimaryKey {
					id: pk_key.primary_key,
					columns,
				};

				result.push(PrimaryKeyInfo {
					def: pk_def,
					shape_id,
				});
			}
		}

		Ok(result)
	}

	pub(crate) fn list_primary_key_columns(rx: &mut Transaction<'_>) -> Result<Vec<(u64, u64, usize)>> {
		let mut result = Vec::new();

		let primary_key_range = {
			let start_key = PrimaryKeyKey::encoded(PrimaryKeyId(u64::MAX));
			let end_key = PrimaryKeyKey::encoded(PrimaryKeyId(0));

			EncodedKeyRange::new(Bound::Included(start_key), Bound::Included(end_key))
		};

		let stream = rx.range(primary_key_range, 1024)?;

		for entry in stream {
			let entry = entry?;

			if let Some(key) = Key::decode(&entry.key)
				&& let Key::PrimaryKey(pk_key) = key
			{
				let column_ids_blob = primary_key::SHAPE.get_blob(&entry.row, primary_key::COLUMN_IDS);
				let column_ids = deserialize_column_ids(&column_ids_blob);

				for (position, column_id) in column_ids.iter().enumerate() {
					result.push((pk_key.primary_key.0, column_id.0, position));
				}
			}
		}

		Ok(result)
	}
}
