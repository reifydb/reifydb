// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{
	ColumnDef, ColumnIndex, NamespaceId, TableVirtualDef,
};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::columns::*, table_virtual::COLUMNS};

/// Returns the static definition for the system.columns virtual table
/// This table exposes information about all columns across all tables and views
pub fn columns() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: COLUMNS,
			namespace: NamespaceId(1), // system namespace
			name: "columns".to_string(),
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
				ColumnDef {
					id: SOURCE_TYPE,
					name: "source_type".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						), // "table" or "view"
					policies: vec![],
					index: ColumnIndex(2),
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
					id: TYPE,
					name: "type".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						),
					policies: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
				},
				ColumnDef {
					id: POSITION,
					name: "position".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Uint4,
						),
					policies: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
				},
				ColumnDef {
					id: AUTO_INCREMENT,
					name: "auto_increment".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Boolean,
						),
					policies: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
