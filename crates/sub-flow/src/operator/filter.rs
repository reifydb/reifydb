// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use reifydb_core::{interface::catalog::flow::FlowNodeId, internal_err, value::column::columns::Columns};
use reifydb_engine::{
	evaluate::{ColumnEvaluationContext, column::StandardColumnEvaluator},
	stack::Stack,
};
use reifydb_rql::expression::Expression;
use reifydb_core::interface::change::{Change, Diff};
use reifydb_type::{
	params::Params,
	value::{Value, row_number::RowNumber},
};

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_STACK: LazyLock<Stack> = LazyLock::new(|| Stack::new());

pub struct FilterOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	conditions: Vec<Expression>,
	column_evaluator: StandardColumnEvaluator,
}

impl FilterOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, conditions: Vec<Expression>) -> Self {
		Self {
			parent,
			node,
			conditions,
			column_evaluator: StandardColumnEvaluator::default(),
		}
	}

	/// Evaluate filter on all rows in Columns
	/// Returns a boolean mask indicating which rows pass the filter
	fn evaluate(&self, columns: &Columns) -> reifydb_type::Result<Vec<bool>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
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

		// Start with all rows passing
		let mut mask = vec![true; row_count];

		for condition in &self.conditions {
			let result_col = self.column_evaluator.evaluate(&ctx, condition)?;

			// Apply the condition to the mask
			for row_idx in 0..row_count {
				if mask[row_idx] {
					match result_col.data().get_value(row_idx) {
						Value::Boolean(true) => {}
						Value::Boolean(false) => mask[row_idx] = false,
						result => {
							return internal_err!(
								"Filter condition did not evaluate to boolean, got: {:?}",
								result
							);
						}
					}
				}
			}
		}

		Ok(mask)
	}

	/// Filter Columns to only include rows that pass the filter
	fn filter_passing(&self, columns: &Columns, mask: &[bool]) -> Columns {
		let passing_indices: Vec<usize> =
			mask.iter().enumerate().filter(|&(_, pass)| *pass).map(|(idx, _)| idx).collect();

		if passing_indices.is_empty() {
			Columns::empty()
		} else {
			columns.extract_by_indices(&passing_indices)
		}
	}

	/// Filter Columns to only include rows that fail the filter
	fn filter_failing(&self, columns: &Columns, mask: &[bool]) -> Columns {
		let failing_indices: Vec<usize> =
			mask.iter().enumerate().filter(|&(_, pass)| !*pass).map(|(idx, _)| idx).collect();

		if failing_indices.is_empty() {
			Columns::empty()
		} else {
			columns.extract_by_indices(&failing_indices)
		}
	}
}

impl Operator for FilterOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: Change,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => {
					let mask = self.evaluate(&post)?;
					let passing = self.filter_passing(&post, &mask);

					if !passing.is_empty() {
						result.push(Diff::Insert {
							post: passing,
						});
					}
				}
				Diff::Update {
					pre,
					post,
				} => {
					// Evaluate filter on the new version
					let mask = self.evaluate(&post)?;
					let passing = self.filter_passing(&post, &mask);
					let failing = self.filter_failing(&post, &mask);

					if !passing.is_empty() {
						// Get the corresponding pre rows for passing post rows
						let passing_indices: Vec<usize> = mask
							.iter()
							.enumerate()
							.filter(|&(_, pass)| *pass)
							.map(|(idx, _)| idx)
							.collect();
						let pre_passing = pre.extract_by_indices(&passing_indices);

						result.push(Diff::Update {
							pre: pre_passing,
							post: passing,
						});
					}

					if !failing.is_empty() {
						// Rows no longer match filter - emit removes for the pre values
						let failing_indices: Vec<usize> = mask
							.iter()
							.enumerate()
							.filter(|&(_, pass)| !*pass)
							.map(|(idx, _)| idx)
							.collect();
						let pre_failing = pre.extract_by_indices(&failing_indices);

						result.push(Diff::Remove {
							pre: pre_failing,
						});
					}
				}
				Diff::Remove {
					pre,
				} => {
					// Always pass through removes
					result.push(Diff::Remove {
						pre,
					});
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, result))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
