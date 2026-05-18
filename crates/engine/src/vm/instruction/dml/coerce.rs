// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::buffer::ColumnBuffer,
};
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{
	Result,
	error::EngineError,
	expression::{cast::cast_column_data, context::EvalContext},
	vm::volcano::query::QueryContext,
};

pub(crate) fn coerce_value_to_column_type(
	value: Value,
	target: Type,
	column: ResolvedColumn,
	ctx: &QueryContext,
) -> Result<Value> {
	if value.get_type() == target {
		return Ok(value);
	}

	if let Type::Option(inner) = &target
		&& value.get_type() == **inner
	{
		return Ok(value);
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

	let temp_column_data = ColumnBuffer::from(value.clone());
	let value_str = value.to_string();

	let base = EvalContext::from_query(ctx);
	let mut eval_ctx = base.with_eval_empty();
	eval_ctx.target = Some(TargetColumn::Resolved(column));
	let coerced_column = cast_column_data(&eval_ctx, &temp_column_data, target, || Fragment::internal(&value_str))?;

	Ok(coerced_column.get_value(0))
}
