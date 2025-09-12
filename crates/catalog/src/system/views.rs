// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{
	ColumnDef, ColumnIndex, NamespaceId, TableVirtualDef,
};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::views::*, table_virtual::VIEWS};

/// Returns the static definition for the system.views virtual table
/// This table exposes information about all views in the database
pub fn views() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: VIEWS,
			namespace: NamespaceId(1), // system namespace
			name: "views".to_string(),
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
					id: NAMESPACE_ID,
					name: "namespace_id".to_string(),
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
					index: ColumnIndex(2),
					auto_increment: false,
				},
				ColumnDef {
					id: KIND,
					name: "kind".to_string(),
					constraint:
						TypeConstraint::unconstrained(
							Type::Utf8,
						), /* Will store
					     * "Deferred" or
					     * "Transactional" */
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
				},
				ColumnDef {
					id: PRIMARY_KEY_ID,
					name: "primary_key_id".to_string(),
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
