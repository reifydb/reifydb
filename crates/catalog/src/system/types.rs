// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::{ColumnId, NamespaceId},
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::vtable::TYPES;

/// Returns the static definition for the system.types virtual table
/// This table exposes information about all supported data types
pub fn types() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: TYPES,
			namespace: NamespaceId::SYSTEM,
			name: "types".to_string(),
			columns: vec![
				Column {
					id: ColumnId(1),
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: ColumnId(2),
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
