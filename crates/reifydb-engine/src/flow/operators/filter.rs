use reifydb_core::{BitVec, interface::Params};
use reifydb_rql::expression::Expression;

use crate::{
	columnar::{ColumnData, Columns},
	evaluate::{EvaluationContext, evaluate},
	flow::{
		change::{Change, Diff},
		operators::{Operator, OperatorContext},
	},
};

pub struct FilterOperator {
	predicate: Expression,
}

impl FilterOperator {
	pub fn new(predicate: Expression) -> Self {
		Self {
			predicate,
		}
	}
}

impl Operator for FilterOperator {
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
					let filtered_columns =
						self.filter(&columns)?;
					if !filtered_columns.is_empty() {
						output.push(Diff::Insert {
							columns:
								filtered_columns,
						});
					}
				}
				Diff::Update {
					old,
					new,
				} => {
					let filtered_new = self.filter(&new)?;
					if !filtered_new.is_empty() {
						output.push(Diff::Update {
							old,
							new: filtered_new,
						});
					} else {
						// If new doesn't pass filter,
						// emit remove of old
						output.push(Diff::Remove {
							columns: old,
						});
					}
				}
				Diff::Remove {
					columns,
				} => {
					// Always pass through removes
					output.push(Diff::Remove {
						columns,
					});
				}
			}
		}

		Ok(Change::new(output))
	}
}

impl FilterOperator {
	fn filter(&self, columns: &Columns) -> crate::Result<Columns> {
		let row_count = columns.row_count();

		// TODO: Flow operator need access to params through
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

		// Evaluate predicate to get boolean column
		let result_column = evaluate(&self.predicate, &eval_ctx)?;
		let mut columns = columns.clone();

		let mut bv = BitVec::repeat(row_count, true);

		match result_column.data() {
			ColumnData::Bool(container) => {
				for (idx, val) in
					container.data().iter().enumerate()
				{
					debug_assert!(
						container.is_defined(idx)
					);
					bv.set(idx, val);
				}
			}
			_ => unreachable!(),
		}

		columns.filter(&bv)?;

		dbg!(&columns);

		Ok(columns)
	}
}
