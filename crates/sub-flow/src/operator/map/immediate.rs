use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{
		CommandTransaction, EvaluationContext, Evaluator, Params,
		expression::Expression,
	},
	value::columnar::Columns,
};

use crate::operator::{Operator, OperatorContext};

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

impl<E: Evaluator> Operator<E> for MapOperator {
	fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &FlowChange,
	) -> crate::Result<FlowChange> {
		let mut output = Vec::new();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					row_ids,
					after,
				} => {
					let projected_columns =
						self.project(ctx, &after)?;
					output.push(FlowDiff::Insert {
						source: *source,
						row_ids: row_ids.clone(),
						after: projected_columns,
					});
				}
				FlowDiff::Update {
					source,
					row_ids,
					before,
					after,
				} => {
					let projected_columns =
						self.project(ctx, &after)?;
					output.push(FlowDiff::Update {
						source: *source,
						row_ids: row_ids.clone(),
						before: before.clone(),
						after: projected_columns,
					});
				}
				FlowDiff::Remove {
					source,
					row_ids,
					before,
				} => {
					// For removes, we might need to project
					// to maintain schema consistency
					let projected_columns =
						self.project(ctx, &before)?;
					output.push(FlowDiff::Remove {
						source: *source,
						row_ids: row_ids.clone(),
						before: projected_columns,
					});
				}
			}
		}

		Ok(FlowChange::new(output))
	}
}

impl MapOperator {
	fn project<E: Evaluator, T: CommandTransaction>(
		&self,
		ctx: &OperatorContext<E, T>,
		columns: &Columns,
	) -> crate::Result<Columns> {
		if columns.is_empty() {
			return Ok(columns.clone());
		}

		let row_count = columns.row_count();

		let empty_params = Params::None;
		let eval_ctx = EvaluationContext {
			target_column: None,
			column_policies: Vec::new(),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &empty_params,
		};

		let mut projected_columns = Vec::new();

		for expr in &self.expressions {
			match ctx.evaluate(&eval_ctx, expr) {
				Ok(column) => {
					projected_columns.push(column);
				}
				Err(e) => {
					return Err(e);
				}
			}
		}

		Ok(Columns::new(projected_columns))
	}
}
