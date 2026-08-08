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
	store::table::{
		decode_table_time,
		shape::{
			table,
			table::{ID, NAME, NAMESPACE, PRIMARY_KEY},
		},
	},
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
	let id = TableId(table::SHAPE.get::<u64>(&bytes, ID));
	let namespace = NamespaceId(table::SHAPE.get::<u64>(&bytes, NAMESPACE));
	let name = table::SHAPE.get_utf8(&bytes, NAME).to_string();

	let partition_by_str = table::SHAPE.get_utf8(&bytes, table::PARTITION_BY);
	let partition_by = if partition_by_str.is_empty() {
		vec![]
	} else {
		partition_by_str.split(',').map(|s| s.to_string()).collect()
	};
	let underlying = table::SHAPE.get::<u8>(&bytes, table::UNDERLYING) != 0;
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
	let pk_id_raw = table::SHAPE.get::<u64>(&multi.bytes, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
