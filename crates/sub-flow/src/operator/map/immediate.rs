use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{EvaluationContext, Evaluator, Transaction, expression::Expression},
	value::columnar::Columns,
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_type::Params;

use crate::operator::Operator;

// Static empty params instance for use in EvaluationContext
static EMPTY_PARAMS: Params = Params::None;

pub struct MapOperator {
	expressions: Vec<Expression<'static>>,
}

impl MapOperator {
	pub fn new(expressions: Vec<Expression<'static>>) -> Self {
		Self {
			expressions,
		}
	}
}

impl<T: Transaction> Operator<T> for MapOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		let mut output = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					rows: row_ids,
					post: after,
				} => {
					let projected_columns = self.project(evaluator, &after)?;
					output.push(FlowDiff::Insert {
						source,
						rows: row_ids.clone(),
						post: projected_columns,
					});
				}
				FlowDiff::Update {
					source,
					rows: row_ids,
					pre: before,
					post: after,
				} => {
					let projected_columns = self.project(evaluator, &after)?;
					output.push(FlowDiff::Update {
						source,
						rows: row_ids.clone(),
						pre: before.clone(),
						post: projected_columns,
					});
				}
				FlowDiff::Remove {
					source,
					rows: row_ids,
					pre: before,
				} => {
					// For removes, we might need to project
					// to maintain namespace consistency
					let projected_columns = self.project(evaluator, &before)?;
					output.push(FlowDiff::Remove {
						source,
						rows: row_ids.clone(),
						pre: projected_columns,
					});
				}
			}
		}

		Ok(FlowChange::new(output))
	}
}

impl MapOperator {
	fn project(&self, evaluator: &StandardEvaluator, columns: &Columns) -> crate::Result<Columns<'static>> {
		if columns.is_empty() {
			// Return empty static columns
			return Ok(Columns::new(Vec::new()));
		}

		let row_count = columns.row_count();

		let eval_ctx = EvaluationContext {
			target: None,
			policies: Vec::new(),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
		};

		let mut projected_columns = Vec::new();

		for expr in &self.expressions {
			let column = evaluator.evaluate(&eval_ctx, expr)?;
			projected_columns.push(column.to_static());
		}

		Ok(Columns::new(projected_columns))
	}
}
