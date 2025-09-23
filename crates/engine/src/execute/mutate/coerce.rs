use reifydb_core::{
	interface::{ResolvedColumn, TargetColumn},
	value::column::{ColumnData, Columns},
};
use reifydb_type::{Fragment, Type, Value};

use crate::{
	evaluate::column::{ColumnEvaluationContext, cast::cast_column_data},
	execute::ExecutionContext,
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
	column: ResolvedColumn<'a>,
	ctx: &ExecutionContext<'a>,
) -> crate::Result<Value> {
	if value.get_type() == target {
		return Ok(value);
	}

	if matches!(value, Value::Undefined) {
		return Ok(value);
	}

	let temp_column_data = ColumnData::from(value.clone());
	let value_str = value.to_string();

	let coerced_column = cast_column_data(
		&ColumnEvaluationContext {
			target: Some(TargetColumn::Resolved(column)),
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &ctx.params,
		},
		&temp_column_data,
		target,
		|| Fragment::owned_internal(&value_str),
	)?;

	Ok(coerced_column.get_value(0))
}
