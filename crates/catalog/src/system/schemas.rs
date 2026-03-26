// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::schemas::*, vtable::SCHEMAS};

/// Returns the static definition for the system.schemas virtual table
/// This table exposes information about all registered schemas in the database
pub fn schemas() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: SCHEMAS,
			namespace: NamespaceId::SYSTEM,
			name: "schemas".to_string(),
			columns: vec![
				Column {
					id: FINGERPRINT,
					name: "fingerprint".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: FIELD_COUNT,
					name: "field_count".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint2),
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
