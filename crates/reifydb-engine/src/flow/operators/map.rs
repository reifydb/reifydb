use reifydb_core::interface::Params;
use reifydb_rql::expression::Expression;

use crate::{
	columnar::Columns,
	evaluate::{EvaluationContext, evaluate},
	flow::{
		change::{Change, Diff},
		operators::{Operator, OperatorContext},
	},
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

impl Operator for MapOperator {
	fn apply(
		&self,
		_ctx: &OperatorContext,
		change: Change,
	) -> crate::Result<Change> {
		let mut output = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					columns,
				} => {
					let projected_columns =
						self.project(&columns)?;
					output.push(Diff::Insert {
						columns: projected_columns,
					});
				}
				Diff::Update {
					old,
					new,
				} => {
					let projected_columns =
						self.project(&new)?;
					output.push(Diff::Update {
						old,
						new: projected_columns,
					});
				}
				Diff::Remove {
					columns,
				} => {
					// For removes, we might need to project
					// to maintain schema consistency
					let projected_columns =
						self.project(&columns)?;
					output.push(Diff::Remove {
						columns: projected_columns,
					});
				}
			}
		}

		Ok(Change::new(output))
	}
}

impl MapOperator {
	fn project(&self, columns: &Columns) -> crate::Result<Columns> {
		if columns.is_empty() {
			return Ok(columns.clone());
		}

		let row_count = columns.row_count();

		// Create evaluation context from input columns
		// TODO: Flow operators need access to params through
		// OperatorContext
		let empty_params = Params::None;
		let eval_ctx = EvaluationContext {
			target_column: None,
			column_policies: Vec::new(),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &empty_params,
		};

		// Evaluate each expression to get projected columns
		let mut projected_columns = Vec::new();
		for expr in &self.expressions {
			let column = evaluate(expr, &eval_ctx)?;
			projected_columns.push(column);
		}

		// Build new columns from projected columns
		Ok(Columns::new(projected_columns))
	}
}
