// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::operator_retention_policies::*, vtable::OPERATOR_RETENTION_POLICIES};

/// Returns the static definition for the system.operator_retention_policies virtual table
/// This table exposes retention policy information for flow operators
pub fn operator_retention_policies() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: OPERATOR_RETENTION_POLICIES,
			namespace: NamespaceId::SYSTEM,
			name: "operator_retention_policies".to_string(),
			columns: vec![
				Column {
					id: OPERATOR_ID,
					name: "operator_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: POLICY_TYPE,
					name: "policy_type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CLEANUP_MODE,
					name: "cleanup_mode".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: VALUE,
					name: "value".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
