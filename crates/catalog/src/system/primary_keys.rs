// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::primary_keys::*, vtable::PRIMARY_KEYS};

/// Returns the static definition for the system.primary_keys virtual table
/// This table exposes information about all primary keys in the database
pub fn primary_keys() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: PRIMARY_KEYS,
			namespace: NamespaceId::SYSTEM,
			name: "primary_keys".to_string(),
			columns: vec![
				Column {
					id: ID,
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: SHAPE_ID,
					name: "shape_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
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
