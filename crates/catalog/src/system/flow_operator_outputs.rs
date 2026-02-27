// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::NamespaceId,
	vtable::VTableDef,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::flow_operator_outputs::*, vtable::FLOW_OPERATOR_OUTPUTS};

/// Returns the static definition for the system.flow_operator_outputs virtual table
/// This table exposes output column definitions for FFI operators
pub fn flow_operator_outputs() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: FLOW_OPERATOR_OUTPUTS,
			namespace: NamespaceId(1), // system namespace
			name: "flow_operator_outputs".to_string(),
			columns: vec![
				ColumnDef {
					id: OPERATOR,
					name: "operator".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: POSITION,
					name: "position".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: NAME,
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: TYPE,
					name: "type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
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
