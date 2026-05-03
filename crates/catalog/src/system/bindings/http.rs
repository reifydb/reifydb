// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, OnceLock};

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::NamespaceId,
	vtable::VTable,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use crate::system::{
	bindings::common_columns,
	ids::{columns::bindings::http::*, vtable::BINDINGS_HTTP},
};

pub fn bindings_http() -> Arc<VTable> {
	static INSTANCE: OnceLock<Arc<VTable>> = OnceLock::new();

	INSTANCE.get_or_init(|| {
		let mut columns = common_columns();
		columns.extend(vec![
			Column {
				id: METHOD,
				name: "method".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Utf8),
				properties: vec![],
				index: ColumnIndex(4),
				auto_increment: false,
				dictionary_id: None,
			},
			Column {
				id: PATH,
				name: "path".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Utf8),
				properties: vec![],
				index: ColumnIndex(5),
				auto_increment: false,
				dictionary_id: None,
			},
			Column {
				id: FORMAT,
				name: "format".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Utf8),
				properties: vec![],
				index: ColumnIndex(6),
				auto_increment: false,
				dictionary_id: None,
			},
		]);

		Arc::new(VTable {
			id: BINDINGS_HTTP,
			namespace: NamespaceId::SYSTEM_BINDINGS,
			name: "http".to_string(),
			columns,
		})
	})
	.clone()
}
