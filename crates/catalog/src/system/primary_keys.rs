// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{
	ColumnDef, ColumnIndex, SchemaId, TableVirtualDef,
};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::primary_keys::*, table_virtual::PRIMARY_KEYS};

/// Returns the static definition for the system.primary_keys virtual table
/// This table exposes information about all primary keys in the database
pub fn primary_keys() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: PRIMARY_KEYS,
			schema: SchemaId(1), // system schema
			name: "primary_keys".to_string(),
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
					id: SOURCE_ID,
					name: "source_id".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Uint8,
						),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
