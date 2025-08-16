use reifydb_core::{
	BitVec,
	interface::{
		Evaluate, EvaluationContext, Params, expression::Expression,
	},
	value::columnar::{ColumnData, Columns},
};

use crate::{
	core::{Change, Diff},
	operator::{Operator, OperatorContext},
};

pub struct FilterOperator {
	conditions: Vec<Expression>,
}

impl FilterOperator {
	pub fn new(conditions: Vec<Expression>) -> Self {
		Self {
			conditions,
		}
	}
}

impl<E: Evaluate> Operator<E> for FilterOperator {
	fn apply(
		&self,
		ctx: &OperatorContext<E>,
		change: Change,
	) -> crate::Result<Change> {
		let mut output = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					after,
				} => {
					let filtered_columns =
						self.filter(ctx, &after)?;
					if !filtered_columns.is_empty() {
						output.push(Diff::Insert {
							after: filtered_columns,
						});
					}
				}
				Diff::Update {
					before,
					after,
				} => {
					let filtered_new =
						self.filter(ctx, &after)?;
					if !filtered_new.is_empty() {
						output.push(Diff::Update {
							before,
							after: filtered_new,
						});
					} else {
						// If new doesn't pass filter,
						// emit remove of old
						output.push(Diff::Remove {
							before,
						});
					}
				}
				Diff::Remove {
					before,
				} => {
					// Always pass through removes
					output.push(Diff::Remove {
						before,
					});
				}
			}
		}

		Ok(Change::new(output))
	}
}

impl FilterOperator {
	fn filter<E: Evaluate>(
		&self,
		ctx: &OperatorContext<E>,
		columns: &Columns,
	) -> crate::Result<Columns> {
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

		// Start with all bits set to true
		let mut final_bv = BitVec::repeat(row_count, true);

		// Evaluate each condition and AND them together
		for condition in &self.conditions {
			let result_column =
				ctx.evaluate(&eval_ctx, condition)?;

			match result_column.data() {
				ColumnData::Bool(container) => {
					for (idx, val) in container
						.data()
						.iter()
						.enumerate()
					{
						debug_assert!(
							container.is_defined(
								idx
							)
						);
						// AND the current condition
						// with the accumulated result
						if !val {
							final_bv.set(
								idx, false,
							);
						}
					}
				}
				_ => unreachable!(),
			}
		}

		let mut columns = columns.clone();
		columns.filter(&final_bv)?;

		Ok(columns)
	}
}
