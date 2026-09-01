// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{column::Column, id::PrimaryKeyId, key::PrimaryKey},
	key::catalog::PrimaryKeyKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	CatalogStore, Result,
	store::primary_key::shape::{deserialize_column_ids, primary_key},
};

pub fn load_primary_keys(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = PrimaryKeyKey::full_scan();

	let mut entries = Vec::new();
	{
		let stream = rx.range(range, RangeScope::All, 1024)?;
		for entry in stream {
			entries.push(entry?);
		}
	}

	for multi in entries {
		let version = multi.version;
		let bytes = EncodedCatalogRow::try_from(multi.bytes)?;

		let pk_id = PrimaryKeyId(primary_key::get_id(&bytes));

		let column_ids_blob = primary_key::get_column_ids(&bytes);
		let column_ids = deserialize_column_ids(&column_ids_blob);

		let mut columns = Vec::new();
		for column_id in column_ids {
			let column = CatalogStore::get_column(rx, column_id)?;
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

		let primary_key = PrimaryKey {
			id: pk_id,
			columns,
		};

		catalog.set_primary_key(pk_id, version, Some(primary_key));
	}

	Ok(())
}
