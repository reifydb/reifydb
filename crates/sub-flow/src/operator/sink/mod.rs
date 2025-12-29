// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod view;

use std::sync::LazyLock;

use reifydb_core::{
	interface::{ColumnDef, ColumnPolicyKind, ColumnSaturationPolicy},
	value::{
		column::{Column, Columns},
		encoded::{EncodedValues, EncodedValuesNamedLayout},
	},
};
use reifydb_engine::{ColumnEvaluationContext, TargetColumn, cast_column_data, stack::Stack};
use reifydb_type::{Fragment, Params, RowNumber};
pub use view::SinkViewOperator;

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(Stack::new);

/// Coerce columns to match target schema types
pub(crate) fn coerce_columns(columns: &Columns, target_columns: &[ColumnDef]) -> crate::Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(Columns::empty());
	}

	let mut result_columns = Vec::with_capacity(target_columns.len());

	for target_col in target_columns {
		let target_type = target_col.constraint.get_type();

		// Create context with Undefined saturation policy for this column
		// This ensures overflow during cast produces undefined instead of errors
		// FIXME how to handle failing views ?!
		let ctx = ColumnEvaluationContext {
			target: Some(TargetColumn::Partial {
				source_name: None,
				column_name: Some(target_col.name.clone()),
				column_type: target_type,
				policies: vec![ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Undefined)],
			}),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
			stack: &EMPTY_STACK,
			is_aggregate_context: false,
		};

		if let Some(source_col) = columns.column(&target_col.name) {
			// Cast to target type
			let casted = cast_column_data(
				&ctx,
				source_col.data(),
				target_type,
				Fragment::internal(&target_col.name),
			)?;
			result_columns.push(Column {
				name: Fragment::internal(&target_col.name),
				data: casted,
			});
		} else {
			result_columns.push(Column::undefined_typed(
				Fragment::internal(&target_col.name),
				target_type,
				row_count,
			))
		}
	}

	// Preserve row numbers
	let row_numbers = columns.row_numbers.iter().cloned().collect();
	Ok(Columns::with_row_numbers(result_columns, row_numbers))
}

/// Encode values at a specific row index directly from Columns without Row allocation
pub(crate) fn encode_row_at_index(
	columns: &Columns,
	row_idx: usize,
	layout: &EncodedValuesNamedLayout,
) -> (RowNumber, EncodedValues) {
	let row_number = columns.row_numbers[row_idx];

	// Collect values for this row from each column
	let values: Vec<reifydb_type::Value> = columns.iter().map(|c| c.data().get_value(row_idx)).collect();

	// Encode directly
	let mut encoded = layout.allocate();
	layout.set_values(&mut encoded, &values);

	(row_number, encoded)
}
