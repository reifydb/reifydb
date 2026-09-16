// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::series::Series, evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{buffer::ColumnBuffer, cast::cast_column_data, columns::Columns},
};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, value_type::ValueType},
};

use crate::{
	Result,
	error::EngineError,
	vm::volcano::query::{QueryContext, eval_context_from_query},
};

pub(crate) fn coerce_value_to_column_type(
	value: Value,
	target: ValueType,
	column: ResolvedColumn,
	ctx: &QueryContext,
) -> Result<Value> {
	if value.get_type() == target {
		return Ok(value);
	}

	if let ValueType::Option(inner) = &target
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

	let base = eval_context_from_query(ctx);
	let mut eval_ctx = base.with_eval_empty();
	eval_ctx.target = Some(TargetColumn::Resolved(column));
	temp_column_data.check_digest_write(&target, || Fragment::internal(&value_str))?;
	let coerced_column = cast_column_data(&eval_ctx, &temp_column_data, target, || Fragment::internal(&value_str))?;

	Ok(coerced_column.get_value(0))
}

pub(crate) fn coerce_series_row(
	series: &Series,
	columns: &Columns,
	context: &QueryContext,
	row_idx: usize,
) -> Result<Vec<Value>> {
	let key_column = series.key.column();
	let mut values = Vec::with_capacity(series.columns.len());
	for column in &series.columns {
		let input = columns.iter().find(|c| c.name().text() == column.name);
		let value = input.map(|c| c.data().get_value(row_idx)).unwrap_or_else(Value::none);
		if column.name == key_column && matches!(value, Value::None { .. }) {
			values.push(value);
			continue;
		}
		let ident = input.map(|c| c.name().clone()).unwrap_or_else(|| Fragment::internal(&column.name));
		let source = context.source.clone().expect("series write context must carry its series as source");
		let resolved = ResolvedColumn::new(ident.clone(), source, column.clone());
		let value = coerce_value_to_column_type(value, column.constraint.get_type(), resolved, context)?;
		if let Err(mut e) = column.constraint.validate(&value) {
			e.0.fragment = ident;
			return Err(e);
		}
		values.push(value);
	}
	Ok(values)
}

pub(crate) fn series_key(series: &Series, value: &Value) -> Result<Option<u64>> {
	if matches!(value, Value::None { .. }) {
		return Ok(None);
	}
	match series.key_to_u64(value.clone()) {
		Some(key) => Ok(Some(key)),
		None => Err(EngineError::SeriesKeyOutOfRange {
			column: series.key.column().to_string(),
			value: value.to_string(),
			fragment: Fragment::internal(value.to_string()),
		}
		.into()),
	}
}
