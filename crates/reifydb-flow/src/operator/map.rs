use reifydb_core::{
	interface::{
		EvaluationContext, Evaluator, Params, Transaction,
		expression::Expression,
	},
	value::columnar::Columns,
};

use crate::{
	core::{Change, Diff},
	operator::{Operator, OperatorContext},
};

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
	fn apply<T: Transaction>(
		&self,
		ctx: &mut OperatorContext<E, T>,
		change: &Change,
	) -> crate::Result<Change> {
		let mut output = Vec::new();

		for diff in &change.diffs {
			match diff {
				Diff::Insert {
					source,
					after,
				} => {
					let projected_columns =
						self.project(ctx, &after)?;
					output.push(Diff::Insert {
						source: *source,
						after: projected_columns,
					});
				}
				Diff::Update {
					source,
					before,
					after,
				} => {
					let projected_columns =
						self.project(ctx, &after)?;
					output.push(Diff::Update {
						source: *source,
						before: before.clone(),
						after: projected_columns,
					});
				}
				Diff::Remove {
					source,
					before,
				} => {
					// For removes, we might need to project
					// to maintain schema consistency
					let projected_columns =
						self.project(ctx, &before)?;
					output.push(Diff::Remove {
						source: *source,
						before: projected_columns,
					});
				}
			}
		}

		Ok(Change::new(output))
	}
}

impl MapOperator {
	fn project<E: Evaluator, T: Transaction>(
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
