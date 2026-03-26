// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::flow_operator_outputs::*, vtable::FLOW_OPERATOR_OUTPUTS};

/// Returns the static definition for the system.flow_operator_outputs virtual table
/// This table exposes output column definitions for FFI operators
pub fn flow_operator_outputs() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: FLOW_OPERATOR_OUTPUTS,
			namespace: NamespaceId::SYSTEM,
			name: "flow_operator_outputs".to_string(),
			columns: vec![
				Column {
					id: OPERATOR,
					name: "operator".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: POSITION,
					name: "position".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
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
					id: DESCRIPTION,
					name: "description".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
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
