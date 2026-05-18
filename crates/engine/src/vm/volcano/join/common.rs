// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use postcard::to_stdvec;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_runtime::hash::{Hash128, xxh3_128};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::Value};

use crate::{
	Result,
	expression::{compile::CompiledExpr, context::EvalContext},
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) fn load_and_merge_all<'a>(
	node: &mut Box<dyn QueryNode>,
	rx: &mut Transaction<'a>,
	ctx: &mut QueryContext,
) -> Result<Columns> {
	let mut result: Option<Columns> = None;

	while let Some(columns) = node.next(rx, ctx)? {
		if let Some(mut acc) = result.take() {
			acc.append_columns(columns)?;
			result = Some(acc);
		} else {
			result = Some(columns);
		}
	}
	let result = result.unwrap_or_else(Columns::empty);
	Ok(result)
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

pub(crate) fn compute_join_hash(
	columns: &Columns,
	col_indices: &[usize],
	row_idx: usize,
	buf: &mut Vec<u8>,
) -> Option<Hash128> {
	buf.clear();
	for &idx in col_indices {
		let value = columns[idx].get_value(row_idx);
		if matches!(value, Value::None { .. }) {
			return None;
		}
		let bytes = to_stdvec(&value).ok()?;
		buf.extend_from_slice(&bytes);
	}
	Some(xxh3_128(buf))
}

pub(crate) fn keys_equal_by_index(
	left: &Columns,
	left_row: usize,
	left_indices: &[usize],
	right: &Columns,
	right_row: usize,
	right_indices: &[usize],
) -> bool {
	for (&li, &ri) in left_indices.iter().zip(right_indices.iter()) {
		let lv = left[li].get_value(left_row);
		let rv = right[ri].get_value(right_row);
		if lv != rv {
			return false;
		}
	}
	true
}

pub(crate) fn eval_join_condition(
	compiled: &[CompiledExpr],
	left_columns: &Columns,
	right_columns: &Columns,
	left_row: &[Value],
	right_row: &[Value],
	alias: &Option<Fragment>,
	ctx: &QueryContext,
) -> bool {
	if compiled.is_empty() {
		return true;
	}
	let eval_columns = build_eval_columns(left_columns, right_columns, left_row, right_row, alias);
	let session = EvalContext::from_query(ctx);
	let exec_ctx = session.with_eval_join(Columns::new(eval_columns));
	compiled.iter().all(|compiled_expr| {
		let col = compiled_expr.execute(&exec_ctx).unwrap();
		matches!(col.data().get_value(0), Value::Boolean(true))
	})
}
