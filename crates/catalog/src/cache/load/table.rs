// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, TableId},
			key::PrimaryKey,
			object::ObjectId,
			table::Table,
		},
		store::MultiVersionRow,
	},
	key::table::TableKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	CatalogStore, Result,
	store::table::{decode_table_time, shape::table},
};

pub(crate) fn load_tables(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = TableKey::full_scan();
	let mut stream = rx.range(range, RangeScope::All, 1024)?;

	let mut tables = Vec::new();
	for entry in stream.by_ref() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_table_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let table = convert_table(multi, primary_key);
		if let Some(id) = pk_id {
			catalog.set_primary_key_object(ObjectId::Table(table.id), id);
		}
		tables.push((table, version));
	}
	drop(stream);

	for (mut table, version) in tables {
		table.columns = CatalogStore::list_columns(rx, table.id)?;
		catalog.set_table(table.id, version, Some(table));
	}

	Ok(())
}

fn convert_table(multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> Table {
	let bytes = multi.bytes;
	let id = TableId(table::get_id(&bytes));
	let namespace = NamespaceId(table::get_namespace(&bytes));
	let name = table::get_name(&bytes).to_string();

	let partition_by_str = table::get_partition_by(&bytes);
	let partition_by = if partition_by_str.is_empty() {
		vec![]
	} else {
		partition_by_str.split(',').map(|s| s.to_string()).collect()
	};
	let underlying = table::get_underlying(&bytes) != 0;
	let time = decode_table_time(&bytes);
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

fn get_table_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = table::get_primary_key(&multi.bytes);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
