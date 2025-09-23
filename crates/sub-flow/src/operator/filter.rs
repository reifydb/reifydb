use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{RowEvaluationContext, RowEvaluator, Transaction, expression::Expression},
	value::row::Row,
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_type::{Params, Value, return_internal_error};

use crate::operator::Operator;

// Static empty params instance for use in EvaluationContext
static EMPTY_PARAMS: Params = Params::None;

pub struct FilterOperator {
	conditions: Vec<Expression<'static>>,
}

impl FilterOperator {
	pub fn new(conditions: Vec<Expression<'static>>) -> Self {
		Self {
			conditions,
		}
	}
}

impl<T: Transaction> Operator<T> for FilterOperator {
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
					source,
					post,
				} => {
					if self.evaluate_row(&post, evaluator)? {
						result.push(FlowDiff::Insert {
							source,
							post,
						});
					}
				}
				FlowDiff::Update {
					source,
					pre,
					post,
				} => {
					// Evaluate filter on the new version
					if self.evaluate_row(&post, evaluator)? {
						// Row still matches filter after update
						result.push(FlowDiff::Update {
							source,
							pre,
							post,
						});
					} else {
						// Row no longer matches filter - emit a remove
						result.push(FlowDiff::Remove {
							source,
							pre,
						});
					}
				}
				FlowDiff::Remove {
					source,
					pre,
				} => {
					// Always pass through removes
					result.push(FlowDiff::Remove {
						source,
						pre,
					});
				}
			}
		}

		Ok(FlowChange::new(result))
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
