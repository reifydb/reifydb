use reifydb_core::{
	OwnedFragment, Type,
	flow::{FlowChange, FlowDiff},
	interface::{
		CommandTransaction, EvaluationContext, Evaluator, Params,
		ViewDef,
		expression::{CastExpression, Expression, TypeExpression},
	},
	value::columnar::{Column, ColumnQualified, Columns},
};

use crate::operator::{Operator, OperatorContext};

pub struct MapTerminalOperator {
	expressions: Vec<Expression>,
	view_def: ViewDef,
}

impl MapTerminalOperator {
	pub fn new(expressions: Vec<Expression>, view_def: ViewDef) -> Self {
		Self {
			expressions,
			view_def,
		}
	}
}

impl<E: Evaluator> Operator<E> for MapTerminalOperator {
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

impl MapTerminalOperator {
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
		for (i, expr) in self.expressions.iter().enumerate() {
			let column =
				if let Some(view_column) =
					self.view_def.columns.get(i)
				{
					// Evaluate the expression first
					let result =
						ctx.evaluate(&eval_ctx, expr)?;
					let current_type =
						result.data().get_type();
					let target_type = view_column.ty;

					// If types don't match and it's not
					// undefined, create a cast expression
					if current_type != target_type
						&& current_type
							!= Type::Undefined
					{
						// Create a cast expression to
						// coerce the type
						let cast_expr = Expression::Cast(CastExpression {
						fragment: OwnedFragment::internal("auto_cast"),
						expression: Box::new(expr.clone()),
						to: TypeExpression {
							fragment: OwnedFragment::internal(target_type.to_string()),
							ty: target_type,
						},
					});

						// Evaluate the cast expression
						let casted = ctx.evaluate(
							&eval_ctx, &cast_expr,
						)?;

						// Create a properly named
						// column
						Column::ColumnQualified(ColumnQualified {
						name: view_column.name.clone(),
						data: casted.data().clone(),
					})
					} else {
						// Types match or it's
						// undefined, just rename if
						// needed
						Column::ColumnQualified(ColumnQualified {
						name: view_column.name.clone(),
						data: result.data().clone(),
					})
					}
				} else {
					// No schema info for this column
					// (shouldn't happen for terminal
					// operator) but we handle it
					// gracefully
					ctx.evaluate(&eval_ctx, expr)?
				};

			projected_columns.push(column);
		}

		Ok(Columns::new(projected_columns))
	}
}
