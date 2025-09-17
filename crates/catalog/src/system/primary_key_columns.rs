// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{ColumnDef, ColumnIndex, NamespaceId, TableVirtualDef};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::primary_key_columns::*, table_virtual::PRIMARY_KEY_COLUMNS};

/// Returns the static definition for the system.primary_key_columns virtual
/// table This table exposes information about columns in primary keys
pub fn primary_key_columns() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: PRIMARY_KEY_COLUMNS,
			namespace: NamespaceId(1), // system namespace
			name: "primary_key_columns".to_string(),
			columns: vec![
				ColumnDef {
					id: PRIMARY_KEY_ID,
					name: "primary_key_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
				},
				ColumnDef {
					id: COLUMN_ID,
					name: "column_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
				ColumnDef {
					id: POSITION,
					name: "position".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
				},
			],
		})
	})
	.clone()
}
