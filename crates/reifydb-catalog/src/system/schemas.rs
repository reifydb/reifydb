// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::{
	Type,
	interface::{ColumnDef, ColumnIndex, SchemaId, TableVirtualDef},
};

use super::ids::{columns::schemas::*, table_virtual::SCHEMAS};

/// Returns the static definition for the system.schemas virtual table
/// This table exposes information about all schemas in the database
pub fn schemas() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: SCHEMAS,
			schema: SchemaId(1), // system schema
			name: "schemas".to_string(),
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
					id: NAME,
					name: "name".to_string(),
					ty: Type::Utf8,
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
