// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{
	ColumnDef, ColumnIndex, SchemaId, TableVirtualDef,
};
use reifydb_type::Type;

use super::ids::{columns::columns::*, table_virtual::COLUMNS};

/// Returns the static definition for the system.columns virtual table
/// This table exposes information about all columns across all tables and views
pub fn columns() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: COLUMNS,
			schema: SchemaId(1), // system schema
			name: "columns".to_string(),
			columns: vec![
				ColumnDef {
					id: ID,
					name: "id".to_string(),
					ty: Type::Uint8,
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
				},
				ColumnDef {
					id: STORE_ID,
					name: "store_id".to_string(),
					ty: Type::Uint8,
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
				ColumnDef {
					id: STORE_TYPE,
					name: "store_type".to_string(),
					ty: Type::Utf8, // "table" or "view"
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
				},
				ColumnDef {
					id: NAME,
					name: "name".to_string(),
					ty: Type::Utf8,
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
				},
				ColumnDef {
					id: TYPE,
					name: "type".to_string(),
					ty: Type::Utf8,
					policies: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
				},
				ColumnDef {
					id: POSITION,
					name: "position".to_string(),
					ty: Type::Uint4,
					policies: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
				},
				ColumnDef {
					id: AUTO_INCREMENT,
					name: "auto_increment".to_string(),
					ty: Type::Bool,
					policies: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
