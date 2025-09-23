// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	MultiVersionQueryTransaction, MultiVersionRow, NamespaceId, PrimaryKeyDef, PrimaryKeyId, TableDef, TableId,
	TableKey,
};

use crate::{MaterializedCatalog, table::layout::table};

pub(crate) fn load_tables(
	qt: &mut impl MultiVersionQueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = TableKey::full_scan();
	let tables = qt.range(range)?;

	for multi in tables {
		let version = multi.version;

		// Extract primary key ID from the table row
		let pk_id = get_table_primary_key_id(&multi);

		// Look up the primary key from the catalog if it exists
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key(id, version));

		// Convert the table with its primary key
		let table_def = convert_table(multi, primary_key);

		catalog.set_table(table_def.id, version, Some(table_def));
	}

	Ok(())
}

fn convert_table(multi: MultiVersionRow, primary_key: Option<PrimaryKeyDef>) -> TableDef {
	let row = multi.row;
	let id = TableId(table::LAYOUT.get_u64(&row, table::ID));
	let namespace = NamespaceId(table::LAYOUT.get_u64(&row, table::NAMESPACE));
	let name = table::LAYOUT.get_utf8(&row, table::NAME).to_string();

	TableDef {
		id,
		name,
		namespace,
		columns: vec![],
		primary_key,
	}
}

fn get_table_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = table::LAYOUT.get_u64(&multi.row, table::PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
