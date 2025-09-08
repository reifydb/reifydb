// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{
	ColumnDef, ColumnIndex, SchemaId, TableVirtualDef,
};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::sequences::*, table_virtual::SEQUENCES};

/// Returns the static definition for the system.sequences virtual table
/// This table exposes information about all sequences in the database
pub fn sequences() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: SEQUENCES,
			schema: SchemaId(1), // system schema
			name: "sequences".to_string(),
			columns: vec![
				ColumnDef {
					id: ID,
					name: "id".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Uint8,
						),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
				},
				ColumnDef {
					id: SCHEMA_ID,
					name: "schema_id".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Uint8,
						),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
				ColumnDef {
					id: NAME,
					name: "name".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						),
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
				},
				ColumnDef {
					id: VALUE,
					name: "value".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Uint8,
						),
					policies: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
