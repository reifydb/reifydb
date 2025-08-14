use reifydb_core::{interface::Params, value::columnar::Columns};
use reifydb_rql::expression::Expression;

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
					after,
				} => {
					let projected_columns =
						self.project(&after)?;
					output.push(Diff::Insert {
						after: projected_columns,
					});
				}
				Diff::Update {
					before,
					after,
				} => {
					let projected_columns =
						self.project(&after)?;
					output.push(Diff::Update {
						before,
						after: projected_columns,
					});
				}
				Diff::Remove {
					before,
				} => {
					// For removes, we might need to project
					// to maintain schema consistency
					let projected_columns =
						self.project(&before)?;
					output.push(Diff::Remove {
						before: projected_columns,
					});
				}
			}
		}

		Ok(Change::new(output))
	}
}

impl MapOperator {
	fn project(&self, columns: &Columns) -> crate::Result<Columns> {
		// if columns.is_empty() {
		// 	return Ok(columns.clone());
		// }
		//
		// let row_count = columns.row_count();
		//
		// // Create evaluation context from input columns
		// // TODO: Flow operator need access to params through
		// // OperatorContext
		// let empty_params = Params::None;
		// let eval_ctx = EvaluationContext {
		// 	target_column: None,
		// 	column_policies: Vec::new(),
		// 	columns: columns.clone(),
		// 	row_count,
		// 	take: None,
		// 	params: &empty_params,
		// };
		//
		// // Evaluate each expression to get projected columns
		// let mut projected_columns = Vec::new();
		// for expr in &self.expressions {
		// 	let column = evaluate(expr, &eval_ctx)?;
		// 	projected_columns.push(column);
		// }
		//
		// // Build new columns from projected columns
		// Ok(Columns::new(projected_columns))

		Ok(columns.clone()) // FIXME remove NOP
	}
}
