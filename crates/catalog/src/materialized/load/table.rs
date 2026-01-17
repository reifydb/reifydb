// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, TableId},
			key::PrimaryKeyDef,
			table::TableDef,
		},
		store::MultiVersionValues,
	},
	key::table::TableKey,
};
use reifydb_transaction::standard::IntoStandardTransaction;

use super::MaterializedCatalog;
use crate::store::table::schema::{
	table,
	table::{ID, NAME, NAMESPACE, PRIMARY_KEY},
};

pub(crate) fn load_tables(rx: &mut impl IntoStandardTransaction, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = TableKey::full_scan();
	let mut stream = txn.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_table_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let table_def = convert_table(multi, primary_key);

		catalog.set_table(table_def.id, version, Some(table_def));
	}

	Ok(())
}

fn convert_table(multi: MultiVersionValues, primary_key: Option<PrimaryKeyDef>) -> TableDef {
	let row = multi.values;
	let id = TableId(table::SCHEMA.get_u64(&row, ID));
	let namespace = NamespaceId(table::SCHEMA.get_u64(&row, NAMESPACE));
	let name = table::SCHEMA.get_utf8(&row, NAME).to_string();

	TableDef {
		id,
		name,
		namespace,
		columns: vec![],
		primary_key,
	}
}

fn get_table_primary_key_id(multi: &MultiVersionValues) -> Option<PrimaryKeyId> {
	let pk_id_raw = table::SCHEMA.get_u64(&multi.values, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
