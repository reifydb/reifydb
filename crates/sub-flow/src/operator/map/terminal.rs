use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{
		EvaluationContext, Evaluator, Params, Transaction, ViewDef,
		expression::{CastExpression, Expression, TypeExpression},
	},
	log_debug, log_error,
	value::columnar::{Column, ColumnQualified, Columns},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_type::{Fragment, Type};

use crate::operator::Operator;

pub struct MapTerminalOperator {
	expressions: Vec<Expression<'static>>,
	view_def: ViewDef,
}

impl MapTerminalOperator {
	pub fn new(
		expressions: Vec<Expression<'static>>,
		view_def: ViewDef,
	) -> Self {
		Self {
			expressions,
			view_def,
		}
	}
}

impl<T: Transaction> Operator<T> for MapTerminalOperator {
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
					after,
				} => {
					let projected_columns = self
						.project(evaluator, &after)?;
					// Only include if we have valid data
					if !projected_columns.is_empty() {
						output.push(FlowDiff::Insert {
							source: *source,
							row_ids: row_ids.clone(),
							after: projected_columns,
						});
					}
				}
				FlowDiff::Update {
					source,
					row_ids,
					before,
					after,
				} => {
					let projected_columns = self
						.project(evaluator, &after)?;
					// Only include if we have valid data
					if !projected_columns.is_empty() {
						output.push(FlowDiff::Update {
							source: *source,
							row_ids: row_ids.clone(),
							before: before.clone(),
							after: projected_columns,
						});
					}
				}
				FlowDiff::Remove {
					source,
					row_ids,
					before,
				} => {
					// For removes, we might need to project
					// to maintain namespace consistency
					let projected_columns = self
						.project(evaluator, &before)?;
					// Only include if we have valid data
					if !projected_columns.is_empty() {
						output.push(FlowDiff::Remove {
							source: *source,
							row_ids: row_ids.clone(),
							before: projected_columns,
						});
					}
				}
			}
		}

		Ok(FlowChange::new(output))
	}
}

impl MapTerminalOperator {
	fn project(
		&self,
		evaluator: &StandardEvaluator,
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

		// For deferred views with joins, we may get partial data when
		// individual tables trigger the flow. We need to detect if we
		// have sufficient data to evaluate all expressions. If any
		// expression evaluation fails due to missing columns, we'll
		// handle it gracefully rather than filtering here.
		// This allows the system to be generic and work with any table
		// names.

		for (i, expr) in self.expressions.iter().enumerate() {
			let column = if let Some(view_column) =
				self.view_def.columns.get(i)
			{
				// Try to evaluate the expression
				// If it fails due to missing columns,
				// we'll handle it
				let result = match evaluator
					.evaluate(&eval_ctx, expr)
				{
					Ok(r) => r,
					Err(e) if e.to_string().contains(
						"column not found",
					) =>
					{
						// This expression references a
						// column that doesn't exist
						// This happens when partial
						// data flows through (e.g.,
						// left side of a join before
						// right side is available)
						// For LEFT JOIN semantics, we
						// should output UNDEFINED for
						// missing columns
						log_debug!(
							"MapTerminal: Column not found for expression {} ({}), using UNDEFINED: {}",
							i,
							view_column.name,
							e
						);

						// Create an undefined column
						// with the correct name
						let undefined_data = reifydb_core::value::columnar::ColumnData::undefined(row_count);
						Column::ColumnQualified(ColumnQualified {
									name: view_column.name.clone(),
									data: undefined_data,
								})
					}
					Err(e) => {
						log_error!(
							"MapTerminal: Error evaluating expression {}: {}",
							i,
							e
						);
						return Err(e);
					}
				};

				let current_type = result.data().get_type();
				let target_type =
					view_column.constraint.get_type();

				// If types don't match and it's not
				// undefined, create a cast expression
				if current_type != target_type
					&& current_type != Type::Undefined
				{
					// Create a cast expression to
					// coerce the type
					let cast_expr = Expression::Cast(CastExpression {
						fragment: Fragment::owned_internal("auto_cast"),
						expression: Box::new(expr.clone()),
						to: TypeExpression {
							fragment: Fragment::owned_internal(target_type.to_string()),
							ty: target_type}});

					// Evaluate the cast expression
					let casted = evaluator.evaluate(
						&eval_ctx, &cast_expr,
					)?;

					// Create a properly named
					// column
					Column::ColumnQualified(
						ColumnQualified {
							name: view_column
								.name
								.clone(),
							data: casted
								.data()
								.clone(),
						},
					)
				} else {
					// Types match or it's
					// undefined, just rename if
					// needed
					Column::ColumnQualified(
						ColumnQualified {
							name: view_column
								.name
								.clone(),
							data: result
								.data()
								.clone(),
						},
					)
				}
			} else {
				// No namespace info for this column
				// (shouldn't happen for terminal
				// operator) but we handle it
				// gracefully
				evaluator.evaluate(&eval_ctx, expr)?
			};

			projected_columns.push(column);
		}

		Ok(Columns::new(projected_columns))
	}
}
