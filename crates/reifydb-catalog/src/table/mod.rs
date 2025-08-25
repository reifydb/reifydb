// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod create;
mod find;
mod get;
mod layout;

pub use create::{TableColumnToCreate, TableToCreate};
use layout::table;
use reifydb_core::interface::{SchemaId, TableDef, TableId, Versioned};

pub(crate) fn convert_table(versioned: Versioned) -> TableDef {
	let row = versioned.row;
	let id = TableId(table::LAYOUT.get_u64(&row, table::ID));
	let schema = SchemaId(table::LAYOUT.get_u64(&row, table::SCHEMA));
	let name = table::LAYOUT.get_utf8(&row, table::NAME).to_string();

	TableDef {
		id,
		name,
		schema,
		columns: vec![], // Columns will be loaded separately if needed
	}
}
