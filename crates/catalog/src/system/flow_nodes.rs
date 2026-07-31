// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

use super::ids::{columns::flow_nodes::*, vtable::FLOW_NODES};

pub fn flow_nodes() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: FLOW_NODES,
			namespace: NamespaceId::SYSTEM,
			name: "flow_nodes".to_string(),
			columns: vec![
				Column {
					id: ID,
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: FLOW_ID,
					name: "flow_id".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: NODE_TYPE,
					name: "node_type".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint1),

					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: DATA,
					name: "data".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Blob),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: STATEFUL,
					name: "stateful".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: RETAINS_FOREVER,
					name: "retains_forever".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: RETENTION_SCALE,
					name: "retention_scale".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Duration),
					properties: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: FRONTIER,
					name: "frontier".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::DateTime),
					properties: vec![],
					index: ColumnIndex(7),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
