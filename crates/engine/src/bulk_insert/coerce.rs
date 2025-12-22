// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Column coercion for bulk inserts.

use reifydb_core::{
	interface::ColumnDef,
	value::column::{ColumnData, Columns},
};
use reifydb_type::{Fragment, Params};

use crate::{
	evaluate::column::{ColumnEvaluationContext, cast::cast_column_data},
	stack::Stack,
};

/// Coerce each column's data to the target type in batch.
pub(super) fn coerce_columns(
	column_data: &[ColumnData],
	columns: &[ColumnDef],
	num_rows: usize,
) -> crate::Result<Vec<ColumnData>> {
	let ctx = ColumnEvaluationContext {
		target: None,
		columns: Columns::empty(),
		row_count: num_rows,
		take: None,
		params: &Params::None,
		stack: &Stack::new(),
		is_aggregate_context: false,
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
