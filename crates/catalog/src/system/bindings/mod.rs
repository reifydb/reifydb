// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod grpc;
pub mod http;
pub mod ws;

use reifydb_core::interface::catalog::{
	column::{Column, ColumnIndex},
	id::ColumnId,
};
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

/// Helper to create common columns for all binding virtual tables.
pub(crate) fn common_columns() -> Vec<Column> {
	vec![
		Column {
			id: ColumnId(1),
			name: "id".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Uint8),
			properties: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		},
		Column {
			id: ColumnId(2),
			name: "namespace_id".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Uint8),
			properties: vec![],
			index: ColumnIndex(1),
			auto_increment: false,
			dictionary_id: None,
		},
		Column {
			id: ColumnId(3),
			name: "procedure_id".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Uint8),
			properties: vec![],
			index: ColumnIndex(2),
			auto_increment: false,
			dictionary_id: None,
		},
		Column {
			id: ColumnId(4),
			name: "name".to_string(),
			constraint: TypeConstraint::unconstrained(Type::Utf8),
			properties: vec![],
			index: ColumnIndex(3),
			auto_increment: false,
			dictionary_id: None,
		},
	]
}
