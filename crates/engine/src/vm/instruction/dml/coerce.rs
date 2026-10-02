// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::RecordBatch;
use reifydb_core::{
	expression::Expression,
	interface::{catalog::series::Series, evaluate::TargetColumn, resolved::ResolvedColumn},
	value::column::{cast::cast_column_data, factory::from_one, write::check_digest_write},
};
use reifydb_evaluate::expression::eval::loses_scale;
use reifydb_rql::query::QueryPlan;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, column_view::ColumnView, system_columns::column_view, value_type::ValueType},
};

use crate::{
	Result,
	error::EngineError,
	vm::volcano::query::{QueryContext, eval_context_from_query},
};

pub(crate) struct InputFragments(HashMap<String, Fragment>);

impl InputFragments {
	pub(crate) fn of(input: &QueryPlan) -> Self {
		let mut fragments = HashMap::new();
		collect_input_fragments(input, &mut fragments);
		Self(fragments)
	}

	pub(crate) fn column(&self, name: &str) -> Fragment {
		self.0.get(name).cloned().unwrap_or_else(|| Fragment::internal(name))
	}
}

fn collect_input_fragments(input: &QueryPlan, fragments: &mut HashMap<String, Fragment>) {
	match input {
		QueryPlan::InlineData(inline) => {
			for alias in inline.rows.iter().flatten() {
				let name = alias.alias.name();
				fragments.entry(name.to_string()).or_insert_with(|| alias.fragment.with_text(name));
			}
		}
		QueryPlan::Patch(patch) => {
			for assignment in &patch.assignments {
				if let Expression::Alias(alias) = assignment {
					fragments
						.entry(alias.alias.name().to_string())
						.or_insert_with(|| alias.alias.0.clone());
				}
			}
		}
		QueryPlan::Extend(extend) => {
			if let Some(input) = &extend.input {
				collect_input_fragments(input, fragments);
			}
		}
		_ => {}
	}
}

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

	if loses_scale(&value, &target) {
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

	let temp_column = from_one("value", value.clone());
	let temp_column_data = ColumnView::try_from(&temp_column)?;
	let value_str = value.to_string();

	let base = eval_context_from_query(ctx);
	let mut eval_ctx = base.with_eval_empty();
	eval_ctx.target = Some(TargetColumn::Resolved(column));
	check_digest_write(&temp_column_data, &target, || Fragment::internal(&value_str))?;
	let coerced_column = cast_column_data(&eval_ctx, &temp_column_data, target, || Fragment::internal(&value_str))?;

	Ok(ColumnView::try_from(&coerced_column)?.get_value(0))
}

pub(crate) fn coerce_series_row(
	series: &Series,
	columns: &RecordBatch,
	fragments: &InputFragments,
	context: &QueryContext,
	row_idx: usize,
) -> Result<Vec<Value>> {
	let key_column = series.key.column();
	let mut values = Vec::with_capacity(series.columns.len());
	for column in &series.columns {
		let input = column_view(columns, &column.name)?;
		let value = input.map(|c| c.get_value(row_idx)).unwrap_or_else(Value::none);
		if column.name == key_column && matches!(value, Value::None { .. }) {
			values.push(value);
			continue;
		}
		let ident = fragments.column(&column.name);
		let source = context.source.clone().expect("series write context must carry its series as source");
		let resolved = ResolvedColumn::new(ident.clone(), source, column.clone());
		let mut value = coerce_value_to_column_type(value, column.constraint.get_type(), resolved, context)?;
		if let Err(mut e) = column.constraint.coerce(&mut value) {
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
