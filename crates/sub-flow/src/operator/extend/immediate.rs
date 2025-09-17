use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{EvaluationContext, Evaluator, Params, Transaction, expression::Expression},
	value::columnar::Columns,
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

use crate::operator::Operator;

pub struct ExtendOperator {
	expressions: Vec<Expression<'static>>,
}

impl ExtendOperator {
	pub fn new(expressions: Vec<Expression<'static>>) -> Self {
		Self {
			expressions,
		}
	}
}

impl<T: Transaction> Operator<T> for ExtendOperator {
	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: &FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		let mut output = Vec::new();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					row_ids,
					after,
				} => {
					let extended_columns = self.extend(evaluator, &after)?;
					output.push(FlowDiff::Insert {
						source: *source,
						row_ids: row_ids.clone(),
						after: extended_columns,
					});
				}
				FlowDiff::Update {
					source,
					row_ids,
					before,
					after,
				} => {
					let extended_before = self.extend(evaluator, &before)?;
					let extended_after = self.extend(evaluator, &after)?;
					output.push(FlowDiff::Update {
						source: *source,
						row_ids: row_ids.clone(),
						before: extended_before,
						after: extended_after,
					});
				}
				FlowDiff::Remove {
					source,
					row_ids,
					before,
				} => {
					let extended_before = self.extend(evaluator, &before)?;
					output.push(FlowDiff::Remove {
						source: *source,
						row_ids: row_ids.clone(),
						before: extended_before,
					});
				}
			}
		}

		Ok(FlowChange::new(output))
	}
}

impl ExtendOperator {
	fn extend(&self, evaluator: &StandardEvaluator, columns: &Columns) -> crate::Result<Columns> {
		// Start with all existing columns (EXTEND preserves everything)
		let mut result_columns = columns.clone().into_iter().collect::<Vec<_>>();
		let row_count = columns.row_count();

		// Add the new derived columns
		let empty_params = Params::None;
		let eval_ctx = EvaluationContext {
			target_column: None,
			column_policies: Vec::new(),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &empty_params,
		};

		for expr in &self.expressions {
			let column = evaluator.evaluate(&eval_ctx, expr)?;
			result_columns.push(column);
		}

		Ok(Columns::new(result_columns))
	}
}
