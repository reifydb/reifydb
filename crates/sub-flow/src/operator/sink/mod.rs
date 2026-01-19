// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod subscription;
pub mod view;

use std::sync::LazyLock;

use reifydb_core::{
	encoded::{encoded::EncodedValues, schema::Schema},
	interface::{
		catalog::{
			column::ColumnDef,
			policy::{ColumnPolicyKind, ColumnSaturationPolicy},
			subscription::SubscriptionColumnDef,
		},
		evaluate::TargetColumn,
	},
	value::column::{Column, columns::Columns},
};
use reifydb_engine::{
	evaluate::{ColumnEvaluationContext, column::cast::cast_column_data},
	stack::Stack,
};
use reifydb_type::{fragment::Fragment, params::Params, value::row_number::RowNumber};
// All types are accessed directly from their submodules:
// - crate::operator::sink::subscription::SinkSubscriptionOperator
// - crate::operator::sink::view::SinkViewOperator

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(Stack::new);

/// Coerce columns to match target schema types
pub(crate) fn coerce_columns(columns: &Columns, target_columns: &[ColumnDef]) -> reifydb_type::Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(Columns::empty());
	}

	// If target columns are empty, use input columns as-is
	if target_columns.is_empty() {
		return Ok(columns.clone());
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

/// Coerce columns to match subscription schema types (simpler than ColumnDef)
pub(crate) fn coerce_subscription_columns(
	columns: &Columns,
	target_columns: &[SubscriptionColumnDef],
) -> reifydb_type::Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(Columns::empty());
	}

	// If target columns are empty (schema-less subscription),
	// use the input columns as-is (inferred from query)
	if target_columns.is_empty() {
		return Ok(columns.clone());
	}

	let mut result_columns = Vec::with_capacity(target_columns.len());

	for target_col in target_columns {
		let target_type = target_col.ty;

		// Create context with Undefined saturation policy for this column
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
pub(crate) fn encode_row_at_index(columns: &Columns, row_idx: usize, schema: &Schema) -> (RowNumber, EncodedValues) {
	let row_number = columns.row_numbers[row_idx];

	// Collect values in SCHEMA FIELD ORDER by matching column names
	// This ensures values are in the same order as schema expects
	let values: Vec<reifydb_type::value::Value> = schema
		.field_names()
		.map(|field_name| {
			// Find column with matching name
			let col = columns
				.iter()
				.find(|col| col.name.as_ref() == field_name)
				.unwrap_or_else(|| panic!("Column '{}' not found in Columns", field_name));

			col.data().get_value(row_idx)
		})
		.collect();

	// Encode directly
	let mut encoded = schema.allocate();
	schema.set_values(&mut encoded, &values);

	(row_number, encoded)
}
