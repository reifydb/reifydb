use reifydb_core::{
	interface::{FlowNodeId, RowEvaluationContext, RowEvaluator, Transaction, expression::Expression},
	value::row::Row,
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_type::{Params, Value, return_internal_error};

use crate::{
	flow::{FlowChange, FlowDiff},
	operator::Operator,
};

// Static empty params instance for use in EvaluationContext
static EMPTY_PARAMS: Params = Params::None;

pub struct FilterOperator {
	node: FlowNodeId,
	conditions: Vec<Expression<'static>>,
}

impl FilterOperator {
	pub fn new(node: FlowNodeId, conditions: Vec<Expression<'static>>) -> Self {
		Self {
			node,
			conditions,
		}
	}
}

impl<T: Transaction> Operator<T> for FilterOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					if self.evaluate_row(&post, evaluator)? {
						result.push(FlowDiff::Insert {
							post,
						});
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Evaluate filter on the new version
					if self.evaluate_row(&post, evaluator)? {
						// Row still matches filter after update
						result.push(FlowDiff::Update {
							pre,
							post,
						});
					} else {
						// Row no longer matches filter - emit a remove
						result.push(FlowDiff::Remove {
							pre,
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
}

impl FilterOperator {
	fn evaluate_row(&self, row: &Row, evaluator: &StandardRowEvaluator) -> crate::Result<bool> {
		let ctx = RowEvaluationContext {
			row: row.clone(),
			target: None,
			params: &EMPTY_PARAMS,
		};

		for condition in &self.conditions {
			match evaluator.evaluate(&ctx, condition)? {
				Value::Boolean(true) => continue,
				Value::Boolean(false) => return Ok(false),
				result => {
					return_internal_error!(
						"Filter condition did not evaluate to boolean, got: {:?}",
						result
					);
				}
			}
		}

		Ok(true)
	}
}
