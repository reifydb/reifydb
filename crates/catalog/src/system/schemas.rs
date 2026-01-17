// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::NamespaceId,
	vtable::VTableDef,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::schemas::*, vtable::SCHEMAS};

/// Returns the static definition for the system.schemas virtual table
/// This table exposes information about all registered schemas in the database
pub fn schemas() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: SCHEMAS,
			namespace: NamespaceId(1), // system namespace
			name: "schemas".to_string(),
			columns: vec![
				ColumnDef {
					id: FINGERPRINT,
					name: "fingerprint".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: FIELD_COUNT,
					name: "field_count".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint2),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
