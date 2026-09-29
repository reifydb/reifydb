// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod grpc;
pub mod http;
pub mod ws;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{interface::catalog::binding::Binding, value::column::builder::ColumnBuilder};
use reifydb_value::value::value_type::ValueType;

pub(crate) fn common_vtable_columns(bindings: &[Binding]) -> Vec<(FieldRef, ArrayRef)> {
	let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, bindings.len());
	let mut namespace_ids = ColumnBuilder::with_capacity(ValueType::Uint8, bindings.len());
	let mut procedure_ids = ColumnBuilder::with_capacity(ValueType::Uint8, bindings.len());
	let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, bindings.len());

	for b in bindings {
		ids.push(*b.id);
		namespace_ids.push(*b.namespace);
		procedure_ids.push(*b.procedure_id);
		names.push(b.name.as_str());
	}

	vec![
		ids.finish("id"),
		namespace_ids.finish("namespace_id"),
		procedure_ids.finish("procedure_id"),
		names.finish("name"),
	]
}
