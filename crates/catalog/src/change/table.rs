// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
};
use reifydb_core::{
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
	store::table::{decode_table_time, shape::table},
};

pub(super) struct TableApplier;

impl CatalogChangeApplier for TableApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let mut table = decode_table(EncodedCatalogRow::view(bytes), &catalog.cache, txn.version());
		table.columns = CatalogStore::list_columns(txn, table.id)?;
		catalog.cache.set_table(table.id, txn.version(), Some(table));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = TableKey::decode(key).map(|k| k.table).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Table,
		})?;
		catalog.cache.set_table(id, txn.version(), None);
		Ok(())
	}
}

use reifydb_core::common::CommitVersion;

use crate::cache::CatalogCache;

fn decode_table(bytes: &EncodedCatalogRow, materialized: &CatalogCache, version: CommitVersion) -> Table {
	let id = TableId(table::get_id(bytes));
	let namespace = NamespaceId(table::get_namespace(bytes));
	let name = table::get_name(bytes).to_string();
	let pk_raw = table::get_primary_key(bytes);
	let primary_key = if pk_raw > 0 {
		materialized.find_primary_key_at(PrimaryKeyId(pk_raw), version)
	} else {
		None
	};
	let partition_by_str = table::get_partition_by(bytes);
	let partition_by = if partition_by_str.is_empty() {
		vec![]
	} else {
		partition_by_str.split(',').map(|s| s.to_string()).collect()
	};
	let underlying = table::get_underlying(bytes) != 0;
	let time = decode_table_time(bytes);
	Table {
		id,
		name,
		namespace,
		columns: vec![],
		primary_key,
		partition_by,
		underlying,
		time,
	}
}
