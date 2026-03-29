// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::shape_fields::*, vtable::SHAPE_FIELDS};

/// Returns the static definition for the system.shape_fields virtual table
/// This table exposes information about all fields across all shapes in the database
pub fn shape_fields() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: SHAPE_FIELDS,
			namespace: NamespaceId::SYSTEM,
			name: "shape_fields".to_string(),
			columns: vec![
				Column {
					id: SHAPE_FINGERPRINT,
					name: "fingerprint".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: FIELD_INDEX,
					name: "field_index".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint2),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: NAME,
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: TYPE,
					name: "type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CONSTRAINT_TYPE,
					name: "constraint_type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					properties: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CONSTRAINT_P1,
					name: "constraint_p1".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					properties: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CONSTRAINT_P2,
					name: "constraint_p2".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					properties: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: OFFSET,
					name: "offset".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					properties: vec![],
					index: ColumnIndex(7),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: SIZE,
					name: "size".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					properties: vec![],
					index: ColumnIndex(8),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: ALIGN,
					name: "align".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					properties: vec![],
					index: ColumnIndex(9),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
