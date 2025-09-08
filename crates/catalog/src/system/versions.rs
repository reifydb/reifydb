// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{
	ColumnDef, ColumnIndex, SchemaId, TableVirtualDef,
};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::versions::*, table_virtual::VERSIONS};

/// Returns the static definition for the system.versions virtual table
/// This table exposes version information about the database system and its
/// components
pub fn versions() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: VERSIONS,
			schema: SchemaId(1), // system schema
			name: "versions".to_string(),
			columns: vec![
				ColumnDef {
					id: NAME,
					name: "name".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
				},
				ColumnDef {
					id: VERSION,
					name: "version".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
				ColumnDef {
					id: DESCRIPTION,
					name: "description".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						),
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
				},
				ColumnDef {
					id: KIND,
					name: "kind".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						),
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
