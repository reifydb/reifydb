// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{
		id::{NamespaceId, PrimaryKeyId, TableId},
		table::Table,
	},
	key::{EncodableKey, kind::KeyKind, table::TableKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::table::schema::table::{self, ID, NAME, NAMESPACE, PRIMARY_KEY},
};

pub(super) struct TableApplier;

impl CatalogChangeApplier for TableApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let mut table = decode_table(row, &catalog.materialized, txn.version());
		table.columns = CatalogStore::list_columns(txn, table.id)?;
		catalog.materialized.set_table(table.id, txn.version(), Some(table));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = TableKey::decode(key).map(|k| k.table).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Table,
		})?;
		catalog.materialized.set_table(id, txn.version(), None);
		Ok(())
	}
}

use reifydb_core::common::CommitVersion;

use crate::materialized::MaterializedCatalog;

fn decode_table(row: &EncodedRow, materialized: &MaterializedCatalog, version: CommitVersion) -> Table {
	let id = TableId(table::SCHEMA.get_u64(row, ID));
	let namespace = NamespaceId(table::SCHEMA.get_u64(row, NAMESPACE));
	let name = table::SCHEMA.get_utf8(row, NAME).to_string();
	let pk_raw = table::SCHEMA.get_u64(row, PRIMARY_KEY);
	let primary_key = if pk_raw > 0 {
		materialized.find_primary_key_at(PrimaryKeyId(pk_raw), version)
	} else {
		None
	};
	Table {
		id,
		name,
		namespace,
		columns: vec![],
		primary_key,
	}
}
