use reifydb_core::{
	flow::{Change, Diff},
	interface::{
		CommandTransaction, EvaluationContext, Evaluator, Params,
		expression::Expression,
	},
	value::columnar::Columns,
};

use crate::operator::{Operator, OperatorContext};

pub struct MapOperator {
	expressions: Vec<Expression>,
}

impl MapOperator {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
		}
	}
}

impl<E: Evaluator> Operator<E> for MapOperator {
	fn apply<T: CommandTransaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &Change,
	) -> crate::Result<Change> {
		let mut output = Vec::new();

		for diff in &change.diffs {
			match diff {
				Diff::Insert {
					source,
					row_ids,
					after,
				} => {
					let projected_columns =
						self.project(ctx, &after)?;
					output.push(Diff::Insert {
						source: *source,
						row_ids: row_ids.clone(),
						after: projected_columns,
					});
				}
				Diff::Update {
					source,
					row_ids,
					before,
					after,
				} => {
					let projected_columns =
						self.project(ctx, &after)?;
					output.push(Diff::Update {
						source: *source,
						row_ids: row_ids.clone(),
						before: before.clone(),
						after: projected_columns,
					});
				}
				Diff::Remove {
					source,
					row_ids,
					before,
				} => {
					// For removes, we might need to project
					// to maintain schema consistency
					let projected_columns =
						self.project(ctx, &before)?;
					output.push(Diff::Remove {
						source: *source,
						row_ids: row_ids.clone(),
						before: projected_columns,
					});
				}
			}
		}

		Ok(Change::new(output))
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
			let column = ctx.evaluate(&eval_ctx, expr)?;
			projected_columns.push(column);
		}

		dbg!(&projected_columns);

		Ok(Columns::new(projected_columns))
	}
}
