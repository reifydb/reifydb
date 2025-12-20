// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{ColumnDef, ColumnIndex, NamespaceId, TableVirtualDef};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::flow_operator_inputs::*, table_virtual::FLOW_OPERATOR_INPUTS};

/// Returns the static definition for the system.flow_operator_inputs virtual table
/// This table exposes input column definitions for FFI operators
pub fn flow_operator_inputs() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: FLOW_OPERATOR_INPUTS,
			namespace: NamespaceId(1), // system namespace
			name: "flow_operator_inputs".to_string(),
			columns: vec![
				ColumnDef {
					id: OPERATOR,
					name: "operator".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: POSITION,
					name: "position".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint1),
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
					id: DESCRIPTION,
					name: "description".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
