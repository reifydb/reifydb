// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_runtime::hash::{Hash128, xxh3_128};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::Value};

use crate::{
	expression::{compile::CompiledExpr, context::EvalContext},
	vm::volcano::query::{QueryContext, QueryNode},
};

/// Load and merge all batches from a node into a single Columns
pub(crate) fn load_and_merge_all<'a>(
	node: &mut Box<dyn QueryNode>,
	rx: &mut Transaction<'a>,
	ctx: &mut QueryContext,
) -> crate::Result<Columns> {
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

/// Result of resolving column names for joins
pub struct ResolvedColumnNames {
	pub qualified_names: Vec<String>,
}

/// Resolve column name conflicts between left and right tables
pub fn resolve_column_names(
	left_columns: &Columns,
	right_columns: &Columns,
	alias: &Option<Fragment>,
	excluded_right_indices: Option<&[usize]>,
) -> ResolvedColumnNames {
	let mut qualified_names = Vec::new();

	// Add left columns (never prefixed)
	for col in left_columns.iter() {
		qualified_names.push(col.name().text().to_string());
	}

	// Add right columns with ALWAYS-prefix behavior
	for (idx, col) in right_columns.iter().enumerate() {
		// Skip excluded columns (used in natural join)
		if let Some(excluded) = excluded_right_indices {
			if excluded.contains(&idx) {
				continue;
			}
		}

		let col_name = col.name().text();

		// ALWAYS prefix right columns with alias (should always be Some now)
		let alias_text = alias.as_ref().map(|a| a.text()).unwrap_or("other");
		let prefixed_name = format!("{}_{}", alias_text, col_name);

		// Check for secondary conflict (prefixed name already exists)
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

/// Build evaluation columns for join conditions
pub fn build_eval_columns(
	left_columns: &Columns,
	right_columns: &Columns,
	left_row: &[Value],
	right_row: &[Value],
	alias: &Option<Fragment>,
) -> Vec<Column> {
	let mut eval_columns = Vec::new();

	for (idx, col) in left_columns.iter().enumerate() {
		let data = match &left_row[idx] {
			Value::None {
				..
			} => ColumnData::typed_none(&col.data().get_type()),
			value => ColumnData::from(value.clone()),
		};
		eval_columns.push(col.with_new_data(data));
	}

	for (idx, col) in right_columns.iter().enumerate() {
		let data = match &right_row[idx] {
			Value::None {
				..
			} => ColumnData::typed_none(&col.data().get_type()),
			value => ColumnData::from(value.clone()),
		};
		if let Some(alias) = alias {
			let aliased_name = Fragment::internal(format!("{}.{}", alias.text(), col.name().text()));
			eval_columns.push(Column {
				name: aliased_name,
				data,
			});
		} else {
			eval_columns.push(col.with_new_data(data));
		}
	}

	eval_columns
}

/// Common context holder for join nodes
pub struct JoinContext {
	pub context: Option<Arc<QueryContext>>,
	pub compiled: Vec<CompiledExpr>,
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

/// Compute a hash over the values at the given column indices for a single row.
/// Returns `None` if any key value is `Undefined` (NULL != NULL semantics).
/// The `buf` parameter is a reusable scratch buffer to avoid per-row allocation.
pub(crate) fn compute_join_hash(
	columns: &Columns,
	col_indices: &[usize],
	row_idx: usize,
	buf: &mut Vec<u8>,
) -> Option<Hash128> {
	buf.clear();
	for &idx in col_indices {
		let value = columns[idx].data().get_value(row_idx);
		if matches!(value, Value::None { .. }) {
			return None;
		}
		let bytes = postcard::to_stdvec(&value).ok()?;
		buf.extend_from_slice(&bytes);
	}
	Some(xxh3_128(buf))
}

/// Check actual key equality between two rows by column indices.
pub(crate) fn keys_equal_by_index(
	left: &Columns,
	left_row: usize,
	left_indices: &[usize],
	right: &Columns,
	right_row: usize,
	right_indices: &[usize],
) -> bool {
	for (&li, &ri) in left_indices.iter().zip(right_indices.iter()) {
		let lv = left[li].data().get_value(left_row);
		let rv = right[ri].data().get_value(right_row);
		if lv != rv {
			return false;
		}
	}
	true
}

/// Evaluate compiled join condition predicates for a (left_row, right_row) pair.
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
	let exec_ctx = EvalContext {
		target: None,
		columns: Columns::new(eval_columns),
		row_count: 1,
		take: Some(1),
		params: &ctx.params,
		symbol_table: &ctx.stack,
		is_aggregate_context: false,
		functions: &ctx.services.functions,
		clock: &ctx.services.clock,
		arena: None,
	};
	compiled.iter().all(|compiled_expr| {
		let col = compiled_expr.execute(&exec_ctx).unwrap();
		matches!(col.data().get_value(0), Value::Boolean(true))
	})
}
