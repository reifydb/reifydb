// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	interface::{Transaction, evaluate::expression::Expression},
	value::column::{Column, ColumnData, Columns, SourceQualified, layout::ColumnsLayout},
};
use reifydb_type::{Fragment, Value};

use crate::{
	StandardTransaction,
	evaluate::{EvaluationContext, evaluate},
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct LeftJoinNode<'a, T: Transaction> {
	left: Box<ExecutionPlan<'a, T>>,
	right: Box<ExecutionPlan<'a, T>>,
	on: Vec<Expression<'a>>,
	alias: Option<Fragment<'a>>,
	layout: Option<ColumnsLayout<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
}

impl<'a, T: Transaction> LeftJoinNode<'a, T> {
	pub fn new(
		left: Box<ExecutionPlan<'a, T>>,
		right: Box<ExecutionPlan<'a, T>>,
		on: Vec<Expression<'a>>,
		alias: Option<Fragment<'a>>,
	) -> Self {
		Self {
			left,
			right,
			on,
			alias,
			layout: None,
			context: None,
		}
	}

	fn load_and_merge_all(
		node: &mut Box<ExecutionPlan<'a, T>>,
		rx: &mut StandardTransaction<'a, T>,
	) -> crate::Result<Columns<'a>> {
		let mut result: Option<Columns> = None;

		while let Some(Batch {
			columns,
		}) = node.next(rx)?
		{
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
}

impl<'a, T: Transaction> QueryNode<'a, T> for LeftJoinNode<'a, T> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a, T>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.left.initialize(rx, ctx)?;
		self.right.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(&mut self, rx: &mut StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "LeftJoinNode::next() called before initialize()");
		let ctx = self.context.as_ref().unwrap();

		if self.layout.is_some() {
			return Ok(None);
		}

		let left_columns = Self::load_and_merge_all(&mut self.left, rx)?;
		let right_columns = Self::load_and_merge_all(&mut self.right, rx)?;

		let left_rows = left_columns.row_count();
		let right_rows = right_columns.row_count();
		let right_width = right_columns.len();

		// Detect column name conflicts
		let left_names: Vec<String> = left_columns.iter().map(|col| col.name().text().to_string()).collect();
		let mut qualified_names = Vec::new();

		// Add left columns (never prefixed)
		for col in left_columns.iter() {
			qualified_names.push(col.name().text().to_string());
		}

		// Add right columns with conflict resolution
		for col in right_columns.iter() {
			let col_name = col.name().text();
			let final_name = if left_names.contains(&col_name.to_string()) {
				// Conflict detected - apply prefixing
				match &self.alias {
					Some(alias) => format!("{}_{}", alias.text(), col_name),
					None => format!("joined_{}", col_name),
				}
			} else {
				// No conflict - keep original name
				col_name.to_string()
			};
			qualified_names.push(final_name);
		}

		let mut result_rows = Vec::new();

		for i in 0..left_rows {
			let left_row = left_columns.get_row(i);

			let mut matched = false;
			for j in 0..right_rows {
				let right_row = right_columns.get_row(j);

				// Build columns for evaluation context
				// For the right side columns, we need to handle aliasing
				let mut eval_columns = Vec::new();

				// Add left columns as-is
				for (idx, col) in left_columns.iter().enumerate() {
					eval_columns.push(col.with_new_data(ColumnData::from(left_row[idx].clone())));
				}

				// Add right columns - if there's an alias, create SourceQualified columns
				for (idx, col) in right_columns.iter().enumerate() {
					if let Some(ref alias) = self.alias {
						// Create a SourceQualified column with the alias
						eval_columns.push(Column::SourceQualified(SourceQualified {
							source: alias.clone(),
							name: col.name().clone(),
							data: ColumnData::from(right_row[idx].clone()),
						}));
					} else {
						eval_columns.push(
							col.with_new_data(ColumnData::from(right_row[idx].clone()))
						);
					}
				}

				let eval_ctx = EvaluationContext {
					target: None,
					columns: Columns::new(eval_columns),
					row_count: 1,
					take: Some(1),
					params: &ctx.params,
				};

				let all_true = self.on.iter().fold(true, |acc, cond| {
					let col = evaluate(&eval_ctx, cond).unwrap();
					matches!(col.data().get_value(0), Value::Boolean(true)) && acc
				});

				if all_true {
					let mut combined = left_row.clone();
					combined.extend(right_row.clone());
					result_rows.push(combined);
					matched = true;
				}
			}

			if !matched {
				let mut combined = left_row.clone();
				combined.extend(vec![Value::Undefined; right_width]);
				result_rows.push(combined);
			}
		}

		// Create columns with conflict-resolved names
		let names_refs: Vec<&str> = qualified_names.iter().map(|s| s.as_str()).collect();
		let columns = Columns::from_rows(&names_refs, &result_rows);

		self.layout = Some(ColumnsLayout::from_columns(&columns));
		Ok(Some(Batch {
			columns,
		}))
	}

	fn layout(&self) -> Option<ColumnsLayout<'a>> {
		self.layout.clone()
	}
}
