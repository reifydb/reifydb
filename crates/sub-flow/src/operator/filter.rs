use reifydb_core::{
	BitVec,
	flow::{FlowChange, FlowDiff},
	interface::{EvaluationContext, Evaluator, Params, Transaction, expression::Expression},
	value::columnar::{ColumnData, Columns},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

use crate::operator::Operator;

pub struct FilterOperator {
	conditions: Vec<Expression<'static>>,
}

impl FilterOperator {
	pub fn new(conditions: Vec<Expression<'static>>) -> Self {
		Self {
			conditions,
		}
	}
}

impl<T: Transaction> Operator<T> for FilterOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: &FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		let mut output = Vec::new();

		for diff in &change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					row_ids,
					post: after,
				} => {
					let (filtered_columns, filtered_indices) =
						self.filter_with_indices(evaluator, &after)?;
					if !filtered_columns.is_empty() {
						// Extract row_ids for the
						// filtered rows
						let mut filtered_row_ids = Vec::new();
						for idx in &filtered_indices {
							filtered_row_ids.push(row_ids[*idx]);
						}
						output.push(FlowDiff::Insert {
							source: *source,
							row_ids: filtered_row_ids,
							post: filtered_columns,
						});
					}
				}
				FlowDiff::Update {
					source,
					row_ids,
					pre: before,
					post: after,
				} => {
					let (filtered_new, filtered_indices) =
						self.filter_with_indices(evaluator, &after)?;
					if !filtered_new.is_empty() {
						// Extract row_ids for the
						// filtered rows
						let mut filtered_row_ids = Vec::new();
						for idx in &filtered_indices {
							filtered_row_ids.push(row_ids[*idx]);
						}
						output.push(FlowDiff::Update {
							source: *source,
							row_ids: filtered_row_ids,
							pre: before.clone(),
							post: filtered_new,
						});
					} else {
						// If new doesn't pass filter,
						// emit remove of old
						output.push(FlowDiff::Remove {
							source: *source,
							row_ids: row_ids.clone(),
							pre: before.clone(),
						});
					}
				}
				FlowDiff::Remove {
					source,
					row_ids,
					pre: before,
				} => {
					// Always pass through removes
					output.push(FlowDiff::Remove {
						source: *source,
						row_ids: row_ids.clone(),
						pre: before.clone(),
					});
				}
			}
		}

		Ok(FlowChange::new(output))
	}
}

impl FilterOperator {
	fn filter(&self, evaluator: &StandardEvaluator, columns: &Columns) -> crate::Result<Columns> {
		let (filtered, _) = self.filter_with_indices(evaluator, columns)?;
		Ok(filtered)
	}

	fn filter_with_indices(
		&self,
		evaluator: &StandardEvaluator,
		columns: &Columns,
	) -> crate::Result<(Columns, Vec<usize>)> {
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
			let result_column = evaluator.evaluate(&eval_ctx, condition)?;

			match result_column.data() {
				ColumnData::Bool(container) => {
					for (idx, val) in container.data().iter().enumerate() {
						debug_assert!(container.is_defined(idx));
						// AND the current condition
						// with the accumulated result
						if !val {
							final_bv.set(idx, false);
						}
					}
				}
				_ => unreachable!(),
			}
		}

		// Collect indices of rows that pass the filter
		let mut indices = Vec::new();
		for (idx, bit) in final_bv.iter().enumerate() {
			if bit {
				indices.push(idx);
			}
		}

		let mut columns = columns.clone();
		columns.filter(&final_bv)?;

		Ok((columns, indices))
	}
}
