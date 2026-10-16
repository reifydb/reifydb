// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

#[derive(Debug, Clone)]
pub struct OperatorColumn {
	pub name: &'static str,
	pub type_constraint: TypeConstraint,
	pub description: &'static str,
}

pub(crate) fn reads_entry_ids(columns: &[OperatorColumn], name: &str) -> bool {
	columns.iter().any(|column| column.name == name && column.type_constraint.get_type() == ValueType::DictionaryId)
}
