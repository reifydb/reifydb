use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{
		CommandTransaction, EvaluationContext, Evaluator, Params,
		expression::Expression,
	},
	value::columnar::Columns,
};

use crate::operator::{Operator, OperatorContext};

pub struct ExtendOperator {
	expressions: Vec<Expression>,
}

impl ExtendOperator {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
		}
	}
}

impl<E: Evaluator> Operator<E> for ExtendOperator {
	fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> crate::Result<FlowChange> {
		let mut output = Vec::new();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					store,
					row_ids,
					after,
				} => {
					let extended_columns =
						self.extend(ctx, &after)?;
					output.push(FlowDiff::Insert {
						store: *store,
						row_ids: row_ids.clone(),
						after: extended_columns,
					});
				}
				FlowDiff::Update {
					store,
					row_ids,
					before,
					after,
				} => {
					let extended_before =
						self.extend(ctx, &before)?;
					let extended_after =
						self.extend(ctx, &after)?;
					output.push(FlowDiff::Update {
						store: *store,
						row_ids: row_ids.clone(),
						before: extended_before,
						after: extended_after,
					});
				}
				FlowDiff::Remove {
					store,
					row_ids,
					before,
				} => {
					let extended_before =
						self.extend(ctx, &before)?;
					output.push(FlowDiff::Remove {
						store: *store,
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
	fn extend<E: Evaluator, T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		columns: &Columns,
	) -> crate::Result<Columns> {
		// Start with all existing columns (EXTEND preserves everything)
		let mut result_columns =
			columns.clone().into_iter().collect::<Vec<_>>();
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
			let column = ctx.evaluate(&eval_ctx, expr)?;
			result_columns.push(column);
		}

		Ok(Columns::new(result_columns))
	}
}
