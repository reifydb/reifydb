// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

use super::ids::{
	columns::operator_libraries::{ABI, CAN_DELETE, CAN_INSERT, CAN_UPDATE, LIBRARY_PATH, OPERATOR_NAME},
	vtable::OPERATOR_LIBRARIES,
};

pub fn operator_libraries() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: OPERATOR_LIBRARIES,
			namespace: NamespaceId::SYSTEM_FLOW_OPERATOR,
			name: "libraries".to_string(),
			columns: vec![
				Column {
					id: OPERATOR_NAME,
					name: "operator_name".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: LIBRARY_PATH,
					name: "library_path".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: ABI,
					name: "abi".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint4),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CAN_INSERT,
					name: "can_insert".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CAN_UPDATE,
					name: "can_update".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CAN_DELETE,
					name: "can_delete".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
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
