// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::policies::*, vtable::POLICIES};

/// Returns the static definition for the system.policies virtual table
/// This table exposes information about all security policies in the database
pub fn policies() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: POLICIES,
			namespace: NamespaceId::SYSTEM,
			name: "policies".to_string(),
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
					id: NAME,
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: TARGET_TYPE,
					name: "target_type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: TARGET_NAMESPACE,
					name: "target_namespace".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: TARGET_SHAPE,
					name: "target_shape".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: ENABLED,
					name: "enabled".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Boolean),
					properties: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
