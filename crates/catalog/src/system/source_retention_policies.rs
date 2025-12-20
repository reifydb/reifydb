// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::{ColumnDef, ColumnIndex, NamespaceId, TableVirtualDef};
use reifydb_type::{Type, TypeConstraint};

use super::ids::{columns::source_retention_policies::*, table_virtual::SOURCE_RETENTION_POLICIES};

/// Returns the static definition for the system.source_retention_policies virtual table
/// This table exposes retention policy information for data sources (tables, views, ring buffers)
pub fn source_retention_policies() -> Arc<TableVirtualDef> {
	static INSTANCE: OnceLock<Arc<TableVirtualDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(TableVirtualDef {
			id: SOURCE_RETENTION_POLICIES,
			namespace: NamespaceId(1), // system namespace
			name: "source_retention_policies".to_string(),
			columns: vec![
				ColumnDef {
					id: SOURCE_ID,
					name: "source_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: SOURCE_TYPE,
					name: "source_type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: POLICY_TYPE,
					name: "policy_type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: CLEANUP_MODE,
					name: "cleanup_mode".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: VALUE,
					name: "value".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
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
