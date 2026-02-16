// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::Value};

use crate::{
	expression::compile::CompiledExpr,
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
			Value::None => ColumnData::typed_none(&col.data().get_type()),
			value => ColumnData::from(value.clone()),
		};
		eval_columns.push(col.with_new_data(data));
	}

	for (idx, col) in right_columns.iter().enumerate() {
		let data = match &right_row[idx] {
			Value::None => ColumnData::typed_none(&col.data().get_type()),
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
