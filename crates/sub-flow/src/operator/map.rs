// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{rc::Rc, sync::LazyLock};

use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	value::column::{Column, columns::Columns},
};
use reifydb_engine::{
	evaluate::{ColumnEvaluationContext, column::StandardColumnEvaluator},
	stack::Stack,
};
use reifydb_rql::expression::Expression;
use reifydb_sdk::flow::{FlowChange, FlowDiff};
use reifydb_type::{fragment::Fragment, params::Params, value::row_number::RowNumber};

use crate::{Operator, operator::Operators, transaction::FlowTransaction};

// Static empty params instance for use in EvaluationContext
static EMPTY_PARAMS: Params = Params::None;
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(|| Stack::new());

pub struct MapOperator {
	parent: Rc<Operators>,
	node: FlowNodeId,
	expressions: Vec<Expression>,
	column_evaluator: StandardColumnEvaluator,
}

impl MapOperator {
	pub fn new(parent: Rc<Operators>, node: FlowNodeId, expressions: Vec<Expression>) -> Self {
		Self {
			parent,
			node,
			expressions,
			column_evaluator: StandardColumnEvaluator::default(),
		}
	}

	/// Project all rows in Columns using expressions
	fn project(&self, columns: &Columns) -> reifydb_type::Result<Columns> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Columns::empty());
		}

		let ctx = ColumnEvaluationContext {
			target: None,
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
			stack: &EMPTY_STACK,
			is_aggregate_context: false,
		};

		let mut result_columns = Vec::with_capacity(self.expressions.len());

		for expr in &self.expressions {
			// Evaluate the expression on the entire batch
			let evaluated_col = self.column_evaluator.evaluate(&ctx, expr)?;

			// Determine the column name from the expression
			let field_name = match expr {
				Expression::Alias(alias_expr) => alias_expr.alias.name().to_string(),
				Expression::Column(col_expr) => col_expr.0.name.text().to_string(),
				Expression::AccessSource(access_expr) => access_expr.column.name.text().to_string(),
				_ => expr.full_fragment_owned().text().to_string(),
			};

			// Create a new column with the proper name
			let named_column = Column {
				name: Fragment::internal(field_name),
				data: evaluated_col.data().clone(),
			};

			result_columns.push(named_column);
		}

		// Preserve row numbers from the input
		let row_numbers = if columns.row_numbers.is_empty() {
			Vec::new()
		} else {
			columns.row_numbers.iter().cloned().collect()
		};

		Ok(Columns::with_row_numbers(result_columns, row_numbers))
	}
}

impl Operator for MapOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
		let mut result = Vec::new();

		for diff in change.diffs.into_iter() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let projected = match self.project(&post) {
						Ok(projected) => projected,
						Err(err) => {
							panic!("{:#?}", err)
						}
					};

					if !projected.is_empty() {
						result.push(FlowDiff::Insert {
							post: projected,
						});
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					let projected_post = self.project(&post)?;
					let projected_pre = self.project(&pre)?;

					if !projected_post.is_empty() {
						result.push(FlowDiff::Update {
							pre: projected_pre,
							post: projected_post,
						});
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					let projected_pre = self.project(&pre)?;
					if !projected_pre.is_empty() {
						result.push(FlowDiff::Remove {
							pre: projected_pre,
						});
					}
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, result))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
