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
	columns::operators::{API, CAP_DELETE, CAP_INSERT, CAP_RECLAIM, CAP_UPDATE, LIBRARY_PATH, OPERATOR},
	vtable::OPERATORS,
};

pub fn operators() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: OPERATORS,
			namespace: NamespaceId::SYSTEM,
			name: "operators".to_string(),
			columns: vec![
				Column {
					id: OPERATOR,
					name: "operator".to_string(),
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
					id: API,
					name: "api".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint4),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CAP_INSERT,
					name: "cap_insert".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CAP_UPDATE,
					name: "cap_update".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CAP_DELETE,
					name: "cap_delete".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: CAP_RECLAIM,
					name: "cap_reclaim".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
