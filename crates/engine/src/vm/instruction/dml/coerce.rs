// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::data::ColumnData,
};
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{
	Result,
	error::EngineError,
	expression::{cast::cast_column_data, context::EvalSession},
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
) -> Result<Value> {
	if value.get_type() == target {
		return Ok(value);
	}

	// For Option targets, accept values matching the inner type
	if let Type::Option(inner) = &target {
		if value.get_type() == **inner {
			return Ok(value);
		}
	}

	if matches!(value, Value::None { .. }) {
		return if target.is_option() {
			Ok(value)
		} else {
			Err(EngineError::NoneNotAllowed {
				fragment: column.identifier().clone(),
				column_type: target,
			}
			.into())
		};
	}

	let temp_column_data = ColumnData::from(value.clone());
	let value_str = value.to_string();

	let session = EvalSession::from_query(ctx);
	let mut eval_ctx = session.eval_empty();
	eval_ctx.target = Some(TargetColumn::Resolved(column));
	let coerced_column = cast_column_data(&eval_ctx, &temp_column_data, target, || Fragment::internal(&value_str))?;

	Ok(coerced_column.get_value(0))
}
