// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

use super::ids::{columns::queue_partitions::*, vtable::QUEUE_PARTITIONS};

pub fn queue_partitions() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: QUEUE_PARTITIONS,
			namespace: NamespaceId::SYSTEM,
			name: "queue_partitions".to_string(),
			columns: vec![
				Column {
					id: QUEUE_ID,
					name: "queue_id".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: PARTITION,
					name: "partition".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint2),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: DEPTH,
					name: "depth".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: IN_FLIGHT,
					name: "in_flight".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: BLOCKED_KEYS,
					name: "blocked_keys".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: OLDEST_DUE_AT,
					name: "oldest_due_at".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::DateTime),
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
