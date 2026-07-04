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
	columns::identity_attribute_values::{ATTRIBUTE, ATTRIBUTE_ID, IDENTITY, VALUE},
	vtable::IDENTITY_ATTRIBUTE_VALUES,
};

pub fn identity_attribute_values() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: IDENTITY_ATTRIBUTE_VALUES,
			namespace: NamespaceId::SYSTEM,
			name: "identity_attribute_values".to_string(),
			columns: vec![
				Column {
					id: IDENTITY,
					name: "identity".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::IdentityId),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: ATTRIBUTE_ID,
					name: "attribute_id".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: ATTRIBUTE,
					name: "attribute".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: VALUE,
					name: "value".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					index: ColumnIndex(3),
					auto_increment: false,
					dictionary_id: None,
				},
			],
		})
	})
	.clone()
}
