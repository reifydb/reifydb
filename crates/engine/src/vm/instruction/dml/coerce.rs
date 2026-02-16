// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{columns::Columns, data::ColumnData},
};
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{
	expression::{cast::cast_column_data, context::EvalContext},
	vm::volcano::query::QueryContext,
};

/// Attempts to coerce a single Value to match the target column type using the
/// existing casting infrastructure
///
/// # Arguments
/// * `value` - The value that needs coercing
/// * `target` - The type of the target table column from namespace
/// * `column` - The resolved column for error reporting and policies
/// * `ctx` - ExecutionContext for accessing params
///
/// # Returns
/// * `Ok(Value)` - Successfully coerced value matching target type
/// * `Err(Error)` - Coercion failed with descriptive error
pub(crate) fn coerce_value_to_column_type<'a>(
	value: Value,
	target: Type,
	column: ResolvedColumn,
	ctx: &QueryContext,
) -> crate::Result<Value> {
	if value.get_type() == target {
		return Ok(value);
	}

	if matches!(value, Value::None { .. }) {
		return Ok(value);
	}

	let temp_column_data = ColumnData::from(value.clone());
	let value_str = value.to_string();

	let coerced_column = cast_column_data(
		&EvalContext {
			target: Some(TargetColumn::Resolved(column)),
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &ctx.params,
			symbol_table: &ctx.stack,
			is_aggregate_context: false,
			functions: &ctx.services.functions,
			clock: &ctx.services.clock,
			arena: None,
		},
		&temp_column_data,
		target,
		|| Fragment::internal(&value_str),
	)?;

	Ok(coerced_column.get_value(0))
}
