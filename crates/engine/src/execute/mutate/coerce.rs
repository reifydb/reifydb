use crate::evaluate::EvaluationContext;
use crate::frame::ColumnValues;
use reifydb_core::{BitVec, Span, Type, Value};

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
    column: &impl Span,
) -> crate::Result<Value> {
    if value.ty() == target {
        return Ok(value);
    }

    if matches!(value, Value::Undefined) {
        return Ok(value);
    }

    let temp_column_values = ColumnValues::from(value);

    let coerced_column = temp_column_values.cast(
        target,
        &EvaluationContext {
            column: None,
            mask: BitVec::new(1, true),
            columns: Vec::new(),
            row_count: 1,
            take: None,
        },
        || column.clone().to_owned(),
    )?;

    Ok(coerced_column.get(0))
}
