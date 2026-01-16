// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::{ColumnId, NamespaceId},
	vtable::VTableDef,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::vtable::FLOW_STORAGE_STATS;

/// Returns the static definition for the system.flow_storage_stats virtual table
pub fn flow_storage_stats() -> Arc<VTableDef> {
	static INSTANCE: OnceLock<Arc<VTableDef>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTableDef {
			id: FLOW_STORAGE_STATS,
			namespace: NamespaceId(1), // system namespace
			name: "flow_storage_stats".to_string(),
			columns: vec![
				ColumnDef {
					id: ColumnId(1),
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(3),
					name: "namespace_id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(4),
					name: "tier".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(5),
					name: "current_key_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(6),
					name: "current_value_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(7),
					name: "current_total_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(8),
					name: "current_count".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(9),
					name: "historical_key_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(7),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(10),
					name: "historical_value_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(8),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(11),
					name: "historical_total_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(9),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(12),
					name: "historical_count".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(10),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(13),
					name: "total_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(11),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(14),
					name: "cdc_key_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(12),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(15),
					name: "cdc_value_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(13),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(16),
					name: "cdc_total_bytes".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(14),
					auto_increment: false,
					dictionary_id: None,
				},
				ColumnDef {
					id: ColumnId(17),
					name: "cdc_count".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					policies: vec![],
					index: ColumnIndex(15),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
