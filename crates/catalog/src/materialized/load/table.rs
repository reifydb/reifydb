// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, TableId},
			key::PrimaryKey,
			table::Table,
		},
		store::MultiVersionRow,
	},
	key::table::TableKey,
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{
	CatalogStore, Result,
	store::table::shape::{
		table,
		table::{ID, NAME, NAMESPACE, PRIMARY_KEY},
	},
};

pub(crate) fn load_tables(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = TableKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	let mut table_rows = Vec::new();
	while let Some(entry) = stream.next() {
		table_rows.push(entry?);
	}
	drop(stream);

	for multi in table_rows {
		let version = multi.version;
		let pk_id = get_table_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let table = convert_table(rx, multi, primary_key)?;
		catalog.set_table(table.id, version, Some(table));
	}

	Ok(())
}

fn convert_table(rx: &mut Transaction<'_>, multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> Result<Table> {
	let row = multi.row;
	let id = TableId(table::SHAPE.get_u64(&row, ID));
	let namespace = NamespaceId(table::SHAPE.get_u64(&row, NAMESPACE));
	let name = table::SHAPE.get_utf8(&row, NAME).to_string();
	let columns = CatalogStore::list_columns(rx, id)?;

	Ok(Table {
		id,
		name,
		namespace,
		columns,
		primary_key,
	})
}

fn get_table_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = table::SHAPE.get_u64(&multi.row, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
