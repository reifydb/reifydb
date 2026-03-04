// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::NamespaceId,
	vtable::VTableDef,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::virtual_table_columns::*, vtable::VIRTUAL_TABLE_COLUMNS};

pub fn virtual_table_columns() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: VIRTUAL_TABLE_COLUMNS,
			namespace: NamespaceId(1),
			name: "virtual_table_columns".to_string(),
			columns: vec![
				ColumnDef {
					id: ID,
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: VTABLE_ID,
					name: "vtable_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: NAME,
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: TYPE,
					name: "type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: POSITION,
					name: "position".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					properties: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
