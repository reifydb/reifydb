use reifydb_core::{
	OwnedFragment, Type,
	flow::{FlowChange, FlowDiff},
	interface::{
		CommandTransaction, EvaluationContext, Evaluator, Params,
		expression::{CastExpression, Expression, TypeExpression},
	},
	value::columnar::{Column, ColumnQualified, Columns},
};

use crate::operator::{Operator, OperatorContext};

pub struct MapOperator {
	expressions: Vec<Expression>,
	target_schema: Option<Vec<(String, Type)>>,
}

impl MapOperator {
	pub fn new(expressions: Vec<Expression>) -> Self {
		Self {
			expressions,
			target_schema: None,
		}
	}

	pub fn with_target_schema(
		mut self,
		schema: Vec<(String, Type)>,
	) -> Self {
		self.target_schema = Some(schema);
		self
	}
}

impl<E: Evaluator> Operator<E> for MapOperator {
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
		for (i, expr) in self.expressions.iter().enumerate() {
			let column = if let Some(ref schema) =
				self.target_schema
			{
				if let Some((col_name, target_type)) =
					schema.get(i)
				{
					// Evaluate the expression first
					let result =
						ctx.evaluate(&eval_ctx, expr)?;
					let current_type =
						result.data().get_type();

					// If types don't match and it's
					// not undefined, create a cast
					// expression
					if current_type != *target_type
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
                                    ty: *target_type,
                                },
                            });

						// Evaluate the cast expression
						let casted = ctx.evaluate(
							&eval_ctx, &cast_expr,
						)?;

						// Create a properly named
						// column
						Column::ColumnQualified(
							ColumnQualified {
								name: col_name
									.clone(
									),
								data: casted
									.data()
									.clone(
									),
							},
						)
					} else {
						// Types match or it's
						// undefined, just rename if
						// needed
						Column::ColumnQualified(
							ColumnQualified {
								name: col_name
									.clone(
									),
								data: result
									.data()
									.clone(
									),
							},
						)
					}
				} else {
					// No schema info for this
					// column, evaluate as-is
					ctx.evaluate(&eval_ctx, expr)?
				}
			} else {
				// No target schema, evaluate as-is
				ctx.evaluate(&eval_ctx, expr)?
			};

			projected_columns.push(column);
		}

		// dbg!(&projected_columns);

		Ok(Columns::new(projected_columns))
	}
}
