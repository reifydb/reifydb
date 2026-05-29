// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

use super::ids::{columns::columns::*, vtable::COLUMNS};

pub fn columns() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: COLUMNS,
			namespace: NamespaceId::SYSTEM,
			name: "columns".to_string(),
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
					id: SHAPE_ID,
					name: "shape_id".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: SHAPE_TYPE,
					name: "shape_type".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint1),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: NAME,
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: TYPE,
					name: "type".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint1),
					properties: vec![],
					index: ColumnIndex(4),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: POSITION,
					name: "position".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint1),
					properties: vec![],
					index: ColumnIndex(5),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: AUTO_INCREMENT,
					name: "auto_increment".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Boolean),
					properties: vec![],
					index: ColumnIndex(6),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: DICTIONARY_ID,
					name: "dictionary_id".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
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
