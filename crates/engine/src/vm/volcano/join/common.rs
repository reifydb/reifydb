// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::FieldRef;
use reifydb_core::{
	error::diagnostic::operation,
	value::{
		batch::{batch, concat, heap_size, take_rows, take_rows_or_none},
		column::{
			factory::{datetime, from_one, rename, typed_none},
			view::group_by::common_key_type,
		},
	},
};
use reifydb_evaluate::expression::compile::CompiledExpr;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	fragment::Fragment,
	value::{
		Value,
		column_view::ColumnView,
		datetime::DateTime,
		system_columns::{
			SystemColumn, is_system_field, stamp_system_columns, system_column, time, user_columns,
		},
		value_type::ValueType,
	},
};

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode, charge_query_memory_bytes, eval_context_from_query},
};

pub(crate) fn load_and_merge_all<'a>(
	node: &mut Box<dyn QueryNode>,
	rx: &mut Transaction<'a>,
	ctx: &mut QueryContext,
) -> Result<RecordBatch> {
	let mut batches = Vec::new();
	let mut charged = 0usize;
	let mut total = 0usize;

	while let Some(columns) = node.next(rx, ctx)? {
		total += heap_size(&columns)?;
		charge_query_memory_bytes(&ctx.memory, &mut charged, total)?;
		batches.push(columns);
	}
	concat(&batches)
}

pub(crate) fn user_views(columns: &RecordBatch) -> Result<Vec<ColumnView<'_>>> {
	user_columns(columns).map(|(field, array)| ColumnView::try_from((array, field.as_ref()))).collect()
}

pub(crate) fn user_row(views: &[ColumnView<'_>], index: usize) -> Vec<Value> {
	views.iter().map(|view| view.get_value(index)).collect()
}

pub(crate) fn user_key_columns<'b>(columns: &'b RecordBatch, indices: &[usize]) -> Vec<(&'b FieldRef, &'b ArrayRef)> {
	let user: Vec<(&FieldRef, &ArrayRef)> = user_columns(columns).collect();
	indices.iter().map(|&index| user[index]).collect()
}

pub(crate) struct JoinSlot<'a> {
	pub columns: &'a RecordBatch,
	pub picks: &'a [usize],
}

pub(crate) fn materialize_join(
	qualified_names: &[String],
	left_slots: &[JoinSlot<'_>],
	right_columns: &RecordBatch,
	right_excluded: &[usize],
	right_picks: &[Option<usize>],
	emitted: u64,
) -> Result<RecordBatch> {
	let parts = left_slots.iter().map(|slot| take_rows(slot.columns, slot.picks)).collect::<Result<Vec<_>>>()?;
	let left = concat(&parts)?;
	let right = take_rows_or_none(right_columns, right_picks)?;
	let mut picked: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(left.num_columns() + right.num_columns());

	for (field, array) in user_columns(&left) {
		let name = &qualified_names[picked.len()];
		picked.push(rename((field.clone(), array.clone()), name));
	}

	for (index, (field, array)) in user_columns(&right).enumerate() {
		if right_excluded.contains(&index) {
			continue;
		}
		let name = &qualified_names[picked.len()];
		picked.push(rename((field.clone(), array.clone()), name));
	}

	for (field, array) in left.schema_ref().fields().iter().zip(left.columns()) {
		if is_system_field(field) {
			picked.push((field.clone(), array.clone()));
		}
	}

	let mut stamps: Vec<(SystemColumn, ArrayRef)> = Vec::new();
	if system_column(&left, SystemColumn::RowNumbers).is_some() {
		let row_numbers = UInt64Array::from_iter_values((1..=right_picks.len() as u64).map(|i| emitted + i));
		stamps.push((SystemColumn::RowNumbers, Arc::new(row_numbers)));
	}
	let left_time = time(&left)?;
	let right_time = time(right_columns)?;
	if !left_time.is_empty() && !right_time.is_empty() {
		let merged: Vec<DateTime> = left_time
			.iter()
			.zip(right_picks)
			.map(|(&time, &pick)| match pick {
				None => time,
				Some(index) => time.max(right_time[index]),
			})
			.collect();
		let (_, array) = datetime(SystemColumn::Time.name(), merged);
		stamps.push((SystemColumn::Time, array));
	}
	stamp_system_columns(batch(picked)?, stamps)
}

pub struct ResolvedColumnNames {
	pub qualified_names: Vec<String>,
}

pub fn resolve_column_names(
	left_columns: &RecordBatch,
	right_columns: &RecordBatch,
	alias: &Option<Fragment>,
	excluded_right_indices: Option<&[usize]>,
) -> ResolvedColumnNames {
	let mut qualified_names = Vec::new();

	for (field, _) in user_columns(left_columns) {
		qualified_names.push(field.name().to_string());
	}

	for (idx, (field, _)) in user_columns(right_columns).enumerate() {
		if let Some(excluded) = excluded_right_indices
			&& excluded.contains(&idx)
		{
			continue;
		}

		let col_name = field.name();

		let alias_text = alias.as_ref().map(|a| a.text()).unwrap_or("other");
		let prefixed_name = format!("{}_{}", alias_text, col_name);

		let mut final_name = prefixed_name.clone();
		if qualified_names.contains(&final_name) {
			let mut counter = 2;
			loop {
				let candidate = format!("{}_{}", prefixed_name, counter);
				if !qualified_names.contains(&candidate) {
					final_name = candidate;
					break;
				}
				counter += 1;
			}
		}

		qualified_names.push(final_name);
	}

	ResolvedColumnNames {
		qualified_names,
	}
}

pub fn build_eval_columns(
	left_columns: &[ColumnView<'_>],
	right_columns: &[ColumnView<'_>],
	left_row: &[Value],
	right_row: &[Value],
	alias: &Option<Fragment>,
) -> Vec<(FieldRef, ArrayRef)> {
	let mut eval_columns = Vec::new();

	for (idx, col) in left_columns.iter().enumerate() {
		let name = col.field.name();
		let data = match &left_row[idx] {
			Value::None {
				..
			} => typed_none(name, &col.get_type()),
			value => from_one(name, value.clone()),
		};
		eval_columns.push(data);
	}

	for (idx, col) in right_columns.iter().enumerate() {
		let name = match alias {
			Some(alias) => format!("{}.{}", alias.text(), col.field.name()),
			None => col.field.name().clone(),
		};
		let data = match &right_row[idx] {
			Value::None {
				..
			} => typed_none(&name, &col.get_type()),
			value => from_one(&name, value.clone()),
		};
		eval_columns.push(data);
	}

	eval_columns
}

pub struct JoinContext {
	pub context: Option<Arc<QueryContext>>,
	pub compiled: Vec<CompiledExpr>,
}

impl Default for JoinContext {
	fn default() -> Self {
		Self::new()
	}
}

impl JoinContext {
	pub fn new() -> Self {
		Self {
			context: None,
			compiled: vec![],
		}
	}

	pub fn set(&mut self, ctx: &QueryContext) {
		self.context = Some(Arc::new(ctx.clone()));
	}

	pub fn get(&self) -> &Arc<QueryContext> {
		self.context.as_ref().expect("Join context not initialized")
	}

	pub fn is_initialized(&self) -> bool {
		self.context.is_some()
	}
}

pub(crate) fn ensure_join_keyable(
	columns: &[ColumnView<'_>],
	key_indices: &[usize],
	fragment: impl Fn(usize) -> Fragment,
) -> Result<()> {
	for (key, &idx) in key_indices.iter().enumerate() {
		let ty = columns[idx].get_type();
		if matches!(ty.inner_type(), ValueType::Digest { .. }) {
			return Err(error!(operation::join_key_unkeyable(fragment(key), ty)));
		}
	}
	Ok(())
}

pub(crate) fn join_key_types(
	left: &[ColumnView<'_>],
	left_indices: &[usize],
	right: &[ColumnView<'_>],
	right_indices: &[usize],
	fragment: impl Fn(usize) -> Fragment,
) -> Result<Vec<ValueType>> {
	let mut targets = Vec::with_capacity(left_indices.len());
	for (key, (&li, &ri)) in left_indices.iter().zip(right_indices.iter()).enumerate() {
		let left_ty = left[li].get_type();
		let right_ty = right[ri].get_type();
		match common_key_type(left_ty.inner_type(), right_ty.inner_type()) {
			Some(target) => targets.push(target),
			None => {
				return Err(error!(operation::join_key_type_mismatch(
					fragment(key),
					left_ty.inner_type().clone(),
					right_ty.inner_type().clone()
				)));
			}
		}
	}
	Ok(targets)
}

pub(crate) fn eval_join_condition(
	compiled: &[CompiledExpr],
	left_columns: &[ColumnView<'_>],
	right_columns: &[ColumnView<'_>],
	left_row: &[Value],
	right_row: &[Value],
	alias: &Option<Fragment>,
	ctx: &QueryContext,
) -> Result<bool> {
	if compiled.is_empty() {
		return Ok(true);
	}
	let eval_columns = build_eval_columns(left_columns, right_columns, left_row, right_row, alias);
	let session = eval_context_from_query(ctx);
	let exec_ctx = session.with_eval_join(batch(eval_columns)?);
	for compiled_expr in compiled {
		let col = compiled_expr.execute(&exec_ctx)?;
		if !matches!(ColumnView::try_from(&col)?.get_value(0), Value::Boolean(true)) {
			return Ok(false);
		}
	}
	Ok(true)
}
