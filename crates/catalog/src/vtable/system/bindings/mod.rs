// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod grpc;
pub mod http;
pub mod ws;

use reifydb_core::{
	interface::catalog::binding::Binding,
	value::column::{Column, data::ColumnData},
};
use reifydb_type::fragment::Fragment;

/// Helper to create the first 4 common columns for any binding virtual table.
pub(crate) fn common_vtable_columns(bindings: &[Binding]) -> Vec<Column> {
	let mut ids = ColumnData::uint8_with_capacity(bindings.len());
	let mut namespace_ids = ColumnData::uint8_with_capacity(bindings.len());
	let mut procedure_ids = ColumnData::uint8_with_capacity(bindings.len());
	let mut names = ColumnData::utf8_with_capacity(bindings.len());

	for b in bindings {
		ids.push(*b.id);
		namespace_ids.push(*b.namespace);
		procedure_ids.push(*b.procedure_id);
		names.push(b.name.as_str());
	}

	vec![
		Column {
			name: Fragment::internal("id"),
			data: ids,
		},
		Column {
			name: Fragment::internal("namespace_id"),
			data: namespace_ids,
		},
		Column {
			name: Fragment::internal("procedure_id"),
			data: procedure_ids,
		},
		Column {
			name: Fragment::internal("name"),
			data: names,
		},
	]
}
