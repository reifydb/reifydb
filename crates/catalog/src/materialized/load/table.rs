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

	let mut tables = Vec::new();
	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_table_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let table = convert_table(multi, primary_key);
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
	let row = multi.row;
	let id = TableId(table::SHAPE.get_u64(&row, ID));
	let namespace = NamespaceId(table::SHAPE.get_u64(&row, NAMESPACE));
	let name = table::SHAPE.get_utf8(&row, NAME).to_string();

	Table {
		id,
		name,
		namespace,
		columns: vec![],
		primary_key,
	}
}

fn get_table_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = table::SHAPE.get_u64(&multi.row, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
