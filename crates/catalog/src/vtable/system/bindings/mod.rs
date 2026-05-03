// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod grpc;
pub mod http;
pub mod ws;

use reifydb_core::{
	interface::catalog::binding::Binding,
	value::column::{ColumnWithName, buffer::ColumnBuffer},
};
use reifydb_type::fragment::Fragment;

pub(crate) fn common_vtable_columns(bindings: &[Binding]) -> Vec<ColumnWithName> {
	let mut ids = ColumnBuffer::uint8_with_capacity(bindings.len());
	let mut namespace_ids = ColumnBuffer::uint8_with_capacity(bindings.len());
	let mut procedure_ids = ColumnBuffer::uint8_with_capacity(bindings.len());
	let mut names = ColumnBuffer::utf8_with_capacity(bindings.len());

	for b in bindings {
		ids.push(*b.id);
		namespace_ids.push(*b.namespace);
		procedure_ids.push(*b.procedure_id);
		names.push(b.name.as_str());
	}

	vec![
		ColumnWithName::new(Fragment::internal("id"), ids),
		ColumnWithName::new(Fragment::internal("namespace_id"), namespace_ids),
		ColumnWithName::new(Fragment::internal("procedure_id"), procedure_ids),
		ColumnWithName::new(Fragment::internal("name"), names),
	]
}
