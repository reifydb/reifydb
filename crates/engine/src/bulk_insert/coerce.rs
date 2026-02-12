// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Column coercion for bulk inserts.

use reifydb_core::{
	interface::catalog::column::ColumnDef,
	value::column::{columns::Columns, data::ColumnData},
};
use reifydb_function::registry::Functions;
use reifydb_runtime::clock::Clock;
use reifydb_type::{fragment::Fragment, params::Params};

use crate::{
	expression::{cast::cast_column_data, context::EvalContext},
	vm::stack::SymbolTable,
};

/// Coerce each column's data to the target type in batch.
pub(super) fn coerce_columns(
	column_data: &[ColumnData],
	columns: &[ColumnDef],
	num_rows: usize,
) -> crate::Result<Vec<ColumnData>> {
	let ctx = EvalContext {
		target: None,
		columns: Columns::empty(),
		row_count: num_rows,
		take: None,
		params: &Params::None,
		symbol_table: &SymbolTable::new(),
		is_aggregate_context: false,
		functions: &Functions::empty(),
		clock: &Clock::default(),
		arena: None,
	};

	let mut coerced_columns: Vec<ColumnData> = Vec::with_capacity(columns.len());

	for (col_idx, col) in columns.iter().enumerate() {
		let target = col.constraint.get_type();
		let source_data = &column_data[col_idx];

		let coerced = cast_column_data(&ctx, source_data, target, || Fragment::internal(&col.name))?;
		coerced_columns.push(coerced);
	}

	Ok(coerced_columns)
}
