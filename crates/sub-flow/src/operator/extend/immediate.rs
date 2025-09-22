use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{EvaluationContext, Evaluator, Transaction, expression::Expression},
	value::columnar::{Column, ColumnQualified, Columns, ResolvedColumn, SourceQualified},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_type::Params;

use crate::operator::Operator;

// Static empty params instance for use in EvaluationContext
static EMPTY_PARAMS: Params = Params::None;

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
		change: FlowChange,
		evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		let mut output = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					rows: row_ids,
					post: after,
				} => {
					let extended_columns = self.extend(evaluator, &after)?;
					output.push(FlowDiff::Insert {
						source,
						rows: row_ids.clone(),
						post: extended_columns,
					});
				}
				FlowDiff::Update {
					source,
					rows: row_ids,
					pre: before,
					post: after,
				} => {
					let extended_before = self.extend(evaluator, &before)?;
					let extended_after = self.extend(evaluator, &after)?;
					output.push(FlowDiff::Update {
						source,
						rows: row_ids.clone(),
						pre: extended_before,
						post: extended_after,
					});
				}
				FlowDiff::Remove {
					source,
					rows: row_ids,
					pre: before,
				} => {
					let extended_before = self.extend(evaluator, &before)?;
					output.push(FlowDiff::Remove {
						source,
						rows: row_ids.clone(),
						pre: extended_before,
					});
				}
			}
		}

		Ok(FlowChange::new(output))
	}
}

impl ExtendOperator {
	fn extend(&self, evaluator: &StandardEvaluator, columns: &Columns) -> crate::Result<Columns<'static>> {
		// Start with all existing columns (EXTEND preserves everything)
		// We need to convert to owned/static columns
		let mut result_columns: Vec<Column<'static>> = Vec::new();
		for col in columns.clone().into_iter() {
			// Convert each column to 'static by making fragments owned while preserving location info
			let static_col = match col {
				Column::Resolved(rc) => Column::Resolved(ResolvedColumn {
					column: rc.column.to_static(),
					data: rc.data.clone(),
				}),
				Column::SourceQualified(sq) => Column::SourceQualified(SourceQualified {
					source: sq.source.to_static(),
					name: sq.name.to_static(),
					data: sq.data.clone(),
				}),
				Column::ColumnQualified(cq) => Column::ColumnQualified(ColumnQualified {
					name: cq.name.to_static(),
					data: cq.data.clone(),
				}),
			};
			result_columns.push(static_col);
		}
		let row_count = columns.row_count();

		// Add the new derived columns
		let eval_ctx = EvaluationContext {
			target_column: None,
			column_policies: Vec::new(),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
		};

		for expr in &self.expressions {
			let column = evaluator.evaluate(&eval_ctx, expr)?;
			// Convert to owned/static column
			let static_col = match column {
				Column::Resolved(rc) => Column::Resolved(ResolvedColumn {
					column: rc.column.to_static(),
					data: rc.data.clone(),
				}),
				Column::SourceQualified(sq) => Column::SourceQualified(SourceQualified {
					source: reifydb_type::Fragment::owned_internal(sq.source.text().to_string()),
					name: reifydb_type::Fragment::owned_internal(sq.name.text().to_string()),
					data: sq.data,
				}),
				Column::ColumnQualified(cq) => Column::ColumnQualified(ColumnQualified {
					name: reifydb_type::Fragment::owned_internal(cq.name.text().to_string()),
					data: cq.data,
				}),
			};
			result_columns.push(static_col);
		}

		Ok(Columns::new(result_columns))
	}
}
