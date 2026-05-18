// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::primary_key_columns::*, vtable::PRIMARY_KEY_COLUMNS};

pub fn primary_key_columns() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: PRIMARY_KEY_COLUMNS,
			namespace: NamespaceId::SYSTEM,
			name: "primary_key_columns".to_string(),
			columns: vec![
				Column {
					id: PRIMARY_KEY_ID,
					name: "primary_key_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: COLUMN_ID,
					name: "column_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: POSITION,
					name: "position".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
