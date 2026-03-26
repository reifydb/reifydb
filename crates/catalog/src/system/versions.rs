// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use super::ids::{columns::versions::*, vtable::VERSIONS};

pub fn versions() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		Arc::new(VTable {
			id: VERSIONS,
			namespace: NamespaceId::SYSTEM,
			name: "versions".to_string(),
			columns: vec![
				Column {
					id: NAME,
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: VERSION,
					name: "version".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: DESCRIPTION,
					name: "description".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					index: ColumnIndex(2),
					auto_increment: false,
					dictionary_id: None,
				},
				Column {
					id: TYPE,
					name: "type".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
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
