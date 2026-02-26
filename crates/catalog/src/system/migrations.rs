// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::NamespaceId,
	vtable::VTableDef,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::migrations::*, vtable::MIGRATIONS};

/// Returns the static definition for the system.migrations virtual table.
/// This table shows migration definitions and their audit trail.
pub fn migrations() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: MIGRATIONS,
			namespace: NamespaceId(1),
			name: "migrations".to_string(),
			columns: vec![
				ColumnDef {
					id: NAME,
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ACTION,
					name: "action".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: BODY,
					name: "body".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ROLLBACK_BODY,
					name: "rollback_body".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
