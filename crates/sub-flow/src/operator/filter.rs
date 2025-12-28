use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use reifydb_core::{interface::FlowNodeId, value::column::Columns};
use reifydb_engine::{ColumnEvaluationContext, StandardColumnEvaluator, stack::Stack};
use reifydb_flow_operator_sdk::{FlowChange, FlowDiff};
use reifydb_rql::expression::Expression;
use reifydb_type::{Params, RowNumber, Value, return_internal_error};

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

// Static empty params instance for use in EvaluationContext
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
	fn evaluate(&self, columns: &Columns) -> crate::Result<Vec<bool>> {
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
							return_internal_error!(
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

#[async_trait]
impl Operator for FilterOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		_txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let mask = self.evaluate(&post)?;
					let passing = self.filter_passing(&post, &mask);

					if !passing.is_empty() {
						result.push(FlowDiff::Insert {
							post: passing,
						});
					}
				}
				FlowDiff::Update {
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

						result.push(FlowDiff::Update {
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

						result.push(FlowDiff::Remove {
							pre: pre_failing,
						});
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					// Always pass through removes
					result.push(FlowDiff::Remove {
						pre,
					});
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, result))
	}

	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns> {
		self.parent.pull(txn, rows).await
	}
}
