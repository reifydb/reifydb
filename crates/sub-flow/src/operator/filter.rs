use std::sync::Arc;

use reifydb_core::{Row, interface::FlowNodeId};
use reifydb_engine::{RowEvaluationContext, StandardRowEvaluator};
use reifydb_flow_operator_sdk::{FlowChange, FlowDiff};
use reifydb_rql::expression::Expression;
use reifydb_type::{Params, RowNumber, Value, return_internal_error};

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

// Static empty params instance for use in EvaluationContext
static EMPTY_PARAMS: Params = Params::None;

pub struct FilterOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	conditions: Vec<Expression>,
}

impl FilterOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, conditions: Vec<Expression>) -> Self {
		Self {
			parent,
			node,
			conditions,
		}
	}
}

impl Operator for FilterOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		_txn: &mut FlowTransaction,
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

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
		self.parent.get_rows(txn, rows)
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
