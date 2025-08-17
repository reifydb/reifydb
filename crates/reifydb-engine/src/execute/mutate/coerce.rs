use reifydb_core::{ColumnDescriptor, OwnedSpan, Type, Value};

use crate::{
	columnar::{ColumnData, Columns},
	evaluate::{EvaluationContext, cast::cast_column_data},
	execute::ExecutionContext,
};

/// Attempts to coerce a single Value to match the target column type using the
/// existing casting infrastructure
///
/// # Arguments
/// * `value` - The value that needs coercing
/// * `target` - The type of the target table column from schema
/// * `column` - Name of the column for error reporting
/// * `ctx` - ExecutionContext for accessing params
///
/// # Returns
/// * `Ok(Value)` - Successfully coerced value matching target type
/// * `Err(Error)` - Coercion failed with descriptive error
pub(crate) fn coerce_value_to_column_type(
	value: Value,
	target: Type,
	column: ColumnDescriptor,
	ctx: &ExecutionContext,
) -> crate::Result<Value> {
	if value.get_type() == target {
		return Ok(value);
	}

	if matches!(value, Value::Undefined) {
		return Ok(value);
	}

	let temp_column_data = ColumnData::from(value.clone());
	let value_str = value.to_string();

	let column_policies = column.policies.clone();

	let coerced_column = cast_column_data(
		&EvaluationContext {
			target_column: Some(column),
			column_policies,
			columns: Columns::empty(),
			row_count: 1,
			take: None,
			params: &ctx.params,
		},
		&temp_column_data,
		target,
		|| OwnedSpan::testing(&value_str),
	)?;

	Ok(coerced_column.get_value(0))
}
