// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::operation,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, view::group_by::common_key_type},
};
use reifydb_evaluate::expression::compile::CompiledExpr;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	fragment::Fragment,
	value::{
		Value, datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns, value_type::ValueType,
	},
};

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode, charge_query_memory, eval_context_from_query},
};

pub(crate) fn load_and_merge_all<'a>(
	node: &mut Box<dyn QueryNode>,
	rx: &mut Transaction<'a>,
	ctx: &mut QueryContext,
) -> Result<Columns> {
	let mut result: Option<Columns> = None;
	let mut charged = 0usize;

	while let Some(columns) = node.next(rx, ctx)? {
		if let Some(mut acc) = result.take() {
			acc.append_columns(columns)?;
			result = Some(acc);
		} else {
			result = Some(columns);
		}
		if let Some(acc) = &result {
			charge_query_memory(&ctx.memory, &mut charged, acc)?;
		}
	}
	let result = result.unwrap_or_else(Columns::empty);
	Ok(result)
}

pub(crate) const NO_MATCH: usize = usize::MAX;

pub(crate) struct JoinSlot<'a> {
	pub columns: &'a [ColumnBuffer],
	pub system: &'a SystemColumns,
	pub picks: &'a [usize],
}

pub(crate) fn materialize_join(
	qualified_names: &[String],
	left_slots: &[JoinSlot<'_>],
	right_columns: &[ColumnBuffer],
	right_picks: &[usize],
	right_time: &[DateTime],
	has_row_numbers: bool,
	emitted: u64,
) -> Result<Columns> {
	let left_width = left_slots.first().map_or(0, |slot| slot.columns.len());
	let mut picked: Vec<ColumnWithName> = Vec::with_capacity(left_width + right_columns.len());

	for index in 0..left_width {
		let parts: Vec<ColumnBuffer> =
			left_slots.iter().map(|slot| slot.columns[index].extract_rows(slot.picks)).collect();
		let name = Fragment::internal(&qualified_names[picked.len()]);
		picked.push(ColumnWithName::new(name, ColumnBuffer::concat(&parts)?));
	}

	for column in right_columns {
		let name = Fragment::internal(&qualified_names[picked.len()]);
		picked.push(ColumnWithName::new(name, column.extract_rows(right_picks)));
	}

	let mut left = SystemColumns::empty();
	for slot in left_slots {
		left.append_indices(slot.system, slot.picks);
	}
	let numbered = has_row_numbers || !left.row_numbers().is_empty();
	let row_numbers = if numbered {
		(1..=right_picks.len() as u64).map(|i| RowNumber(emitted + i)).collect()
	} else {
		Vec::new()
	};
	let time = if left.time().is_empty() || right_time.is_empty() {
		left.time().to_vec()
	} else {
		left.time()
			.iter()
			.zip(right_picks)
			.map(|(&time, &pick)| {
				if pick == NO_MATCH {
					time
				} else {
					time.max(right_time[pick])
				}
			})
			.collect()
	};
	let mut system = SystemColumns::new(
		row_numbers,
		left.partitions().to_vec(),
		left.created_at().to_vec(),
		left.updated_at().to_vec(),
		time,
		left.commit_versions().to_vec(),
	);
	if numbered {
		system.mark_row_numbers();
	}
	Ok(Columns::with_system(picked, system))
}

pub struct ResolvedColumnNames {
	pub qualified_names: Vec<String>,
}

pub fn resolve_column_names(
	left_columns: &Columns,
	right_columns: &Columns,
	alias: &Option<Fragment>,
	excluded_right_indices: Option<&[usize]>,
) -> ResolvedColumnNames {
	let mut qualified_names = Vec::new();

	for col in left_columns.iter() {
		qualified_names.push(col.name().text().to_string());
	}

	for (idx, col) in right_columns.iter().enumerate() {
		if let Some(excluded) = excluded_right_indices
			&& excluded.contains(&idx)
		{
			continue;
		}

		let col_name = col.name().text();

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
	left_columns: &Columns,
	right_columns: &Columns,
	left_row: &[Value],
	right_row: &[Value],
	alias: &Option<Fragment>,
) -> Vec<ColumnWithName> {
	let mut eval_columns = Vec::new();

	for (idx, col) in left_columns.iter().enumerate() {
		let data = match &left_row[idx] {
			Value::None {
				..
			} => ColumnBuffer::typed_none(&col.get_type()),
			value => ColumnBuffer::from(value.clone()),
		};
		eval_columns.push(ColumnWithName::new(col.name().clone(), data));
	}

	for (idx, col) in right_columns.iter().enumerate() {
		let data = match &right_row[idx] {
			Value::None {
				..
			} => ColumnBuffer::typed_none(&col.get_type()),
			value => ColumnBuffer::from(value.clone()),
		};
		if let Some(alias) = alias {
			let aliased_name = Fragment::internal(format!("{}.{}", alias.text(), col.name().text()));
			eval_columns.push(ColumnWithName {
				name: aliased_name,
				data,
			});
		} else {
			eval_columns.push(ColumnWithName::new(col.name().clone(), data));
		}
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

pub(crate) fn ensure_join_keyable(columns: &Columns, key_indices: &[usize]) -> Result<()> {
	for &idx in key_indices {
		let ty = columns[idx].get_type();
		if matches!(ty.inner_type(), ValueType::Digest { .. }) {
			return Err(error!(operation::join_key_unkeyable(columns.name_at(idx).clone(), ty)));
		}
	}
	Ok(())
}

pub(crate) fn join_key_types(
	left: &Columns,
	left_indices: &[usize],
	right: &Columns,
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
	left_columns: &Columns,
	right_columns: &Columns,
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
	let exec_ctx = session.with_eval_join(Columns::new(eval_columns));
	for compiled_expr in compiled {
		let col = compiled_expr.execute(&exec_ctx)?;
		if !matches!(col.data().get_value(0), Value::Boolean(true)) {
			return Ok(false);
		}
	}
	Ok(true)
}
