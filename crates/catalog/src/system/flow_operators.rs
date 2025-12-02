// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{ColumnDef, ColumnIndex, NamespaceId, TableVirtualDef};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::flow_operators::*, table_virtual::FLOW_OPERATORS};

/// Returns the static definition for the system.flow_operators virtual table
/// This table exposes information about loaded FFI operators from shared libraries
pub fn flow_operators() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: FLOW_OPERATORS,
			namespace: NamespaceId(1), // system namespace
			name: "flow_operators".to_string(),
			columns: vec![
				ColumnDef {
					id: OPERATOR_NAME,
					name: "operator_name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: LIBRARY_PATH,
					name: "library_path".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: API_VERSION,
					name: "api_version".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
