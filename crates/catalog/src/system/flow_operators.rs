// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{ColumnDef, ColumnIndex, NamespaceId, VTableDef};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::flow_operators::*, vtable::FLOW_OPERATORS};

/// Returns the static definition for the system.flow_operators virtual table
/// This table exposes information about loaded FFI operators from shared libraries
pub fn flow_operators() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: FLOW_OPERATORS,
			namespace: NamespaceId(1), // system namespace
			name: "flow_operators".to_string(),
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
					id: LIBRARY_PATH,
					name: "library_path".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: API,
					name: "api".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint4),
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CAP_INSERT,
					name: "cap_insert".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Boolean),
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CAP_UPDATE,
					name: "cap_update".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Boolean),
					policies: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CAP_DELETE,
					name: "cap_delete".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Boolean),
					policies: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CAP_DROP,
					name: "cap_drop".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Boolean),
					policies: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CAP_PULL,
					name: "cap_pull".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Boolean),
					policies: vec![],
					index: ColumnIndex(7),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CAP_TICK,
					name: "cap_tick".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Boolean),
					policies: vec![],
					index: ColumnIndex(8),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
