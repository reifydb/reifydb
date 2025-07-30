use crate::column::EngineColumnData;
use crate::evaluate::EvaluationContext;
use crate::evaluate::cast::cast_column_data;
use reifydb_core::{BorrowedSpan, ColumnDescriptor, Span, Type, Value};

/// Attempts to coerce a single Value to match the target column type using the existing casting infrastructure
///
/// # Arguments
/// * `value` - The value that needs coercing
/// * `target` - The type of the target table column from schema
/// * `column` - Name of the column for error reporting
///
/// # Returns
/// * `Ok(Value)` - Successfully coerced value matching target type
/// * `Err(Error)` - Coercion failed with descriptive error
pub(crate) fn coerce_value_to_column_type(
    value: Value,
    target: Type,
    column: ColumnDescriptor,
) -> crate::Result<Value> {
    if value.get_type() == target {
        return Ok(value);
    }

    if matches!(value, Value::Undefined) {
        return Ok(value);
    }

    let temp_column_data = EngineColumnData::from(value.clone());
    let value_str = value.to_string();

    let column_policies = column.policies.clone();

    let coerced_column = cast_column_data(
        &temp_column_data,
        target,
        &EvaluationContext {
            target_column: Some(column),
            column_policies,
            columns: Vec::new(),
            row_count: 1,
            take: None,
        },
        || BorrowedSpan::new(&value_str).to_owned(),
    )?;

    Ok(coerced_column.get_value(0))
}
