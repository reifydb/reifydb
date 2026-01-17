// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::NamespaceId,
	vtable::VTableDef,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::schema_fields::*, vtable::SCHEMA_FIELDS};

/// Returns the static definition for the system.schema_fields virtual table
/// This table exposes information about all fields across all schemas in the database
pub fn schema_fields() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: SCHEMA_FIELDS,
			namespace: NamespaceId(1), // system namespace
			name: "schema_fields".to_string(),
			columns: vec![
				ColumnDef {
					id: SCHEMA_FINGERPRINT,
					name: "fingerprint".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: FIELD_INDEX,
					name: "field_index".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint2),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: NAME,
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: TYPE,
					name: "type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CONSTRAINT_TYPE,
					name: "constraint_type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					policies: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CONSTRAINT_P1,
					name: "constraint_p1".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					policies: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CONSTRAINT_P2,
					name: "constraint_p2".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					policies: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: OFFSET,
					name: "offset".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					policies: vec![],
					index: ColumnIndex(7),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: SIZE,
					name: "size".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					policies: vec![],
					index: ColumnIndex(8),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ALIGN,
					name: "align".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					policies: vec![],
					index: ColumnIndex(9),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
