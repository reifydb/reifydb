// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{FlowNodeId, RowEvaluationContext, RowEvaluator, Transaction, expression::Expression},
	value::row::{EncodedRowNamedLayout, Row},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_type::{Params, Type};

use crate::Operator;

// Static empty params instance for use in RowEvaluationContext
static EMPTY_PARAMS: Params = Params::None;

pub struct MapOperator {
	node: FlowNodeId,
	expressions: Vec<Expression<'static>>,
}

impl MapOperator {
	pub fn new(node: FlowNodeId, expressions: Vec<Expression<'static>>) -> Self {
		Self {
			node,
			expressions,
		}
	}
}

impl<T: Transaction> Operator<T> for MapOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		println!(
			"MAP[{:?}]: Applying change with {} diffs from {:?}",
			self.node,
			change.diffs.len(),
			change.origin
		);

		let mut result = Vec::new();

		for (i, diff) in change.diffs.into_iter().enumerate() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					println!(
						"MAP[{:?}]: Processing INSERT with {} input columns",
						self.node,
						post.layout.fields.len()
					);

					// let projected = self.project_row(&post, evaluator)?;

					let projected = match self.project_row(&post, evaluator) {
						Ok(projected) => projected,
						Err(err) => {
							println!("MAP[{:?}]: {}", self.node, err);
							panic!("{:#?}", err)
						}
					};

					println!(
						"MAP[{:?}]: Projected to {} output columns",
						self.node,
						projected.layout.fields.len()
					);
					result.push(FlowDiff::Insert {
						post: projected,
					});
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					println!(
						"MAP[{:?}]: Processing UPDATE with {} input columns",
						self.node,
						post.layout.fields.len()
					);
					let projected = self.project_row(&post, evaluator)?;
					println!(
						"MAP[{:?}]: Projected to {} output columns",
						self.node,
						projected.layout.fields.len()
					);
					result.push(FlowDiff::Update {
						pre,
						post: projected,
					});
				}
				FlowDiff::Remove {
					pre,
				} => {
					// pass through
					result.push(FlowDiff::Remove {
						pre,
					});
				}
			}
		}

		println!("MAP[{:?}]: Returning {} diffs", self.node, result.len());
		Ok(FlowChange::internal(self.node, result))
	}
}

impl MapOperator {
	fn project_row(&self, row: &Row, evaluator: &StandardRowEvaluator) -> crate::Result<Row> {
		let ctx = RowEvaluationContext {
			row: row.clone(),
			target: None,
			params: &EMPTY_PARAMS,
		};

		let mut values = Vec::with_capacity(self.expressions.len());
		let mut field_names = Vec::with_capacity(self.expressions.len());
		let mut field_types = Vec::with_capacity(self.expressions.len());

		for (i, expr) in self.expressions.iter().enumerate() {
			// Try to evaluate the expression normally first
			let value = match evaluator.evaluate(&ctx, expr) {
				Ok(v) => v,
				Err(e) => {
					// If it's an AccessSource expression and evaluation failed,
					// try to evaluate just the column name without the source
					if let Expression::AccessSource(access_expr) = expr {
						let col_name = access_expr.column.name.text();

						// Find the column by name in the row
						let names = row.layout.names();
						if let Some(col_idx) = names.iter().position(|n| n == col_name) {
							row.layout.get_value(&row.encoded, col_idx)
						} else {
							return Err(e);
						}
					} else if let Expression::Alias(alias_expr) = expr {
						// For alias expressions, try to handle the inner expression
						if let Expression::AccessSource(access_expr) = &*alias_expr.expression {
							let col_name = access_expr.column.name.text();

							// Find the column by name in the row
							let names = row.layout.names();
							if let Some(col_idx) = names.iter().position(|n| n == col_name)
							{
								row.layout.get_value(&row.encoded, col_idx)
							} else {
								return Err(e);
							}
						} else {
							return Err(e);
						}
					} else {
						return Err(e);
					}
				}
			};

			values.push(value.clone());

			let field_name = match expr {
				Expression::Alias(alias_expr) => alias_expr.alias.name().to_string(),
				Expression::Column(col_expr) => col_expr.0.name.text().to_string(),
				Expression::AccessSource(access_expr) => access_expr.column.name.text().to_string(),
				_ => expr.full_fragment_owned().text().to_string(),
			};

			let field_type = value.get_type();

			field_names.push(field_name);
			field_types.push(field_type);
		}

		let fields: Vec<(String, Type)> = field_names.into_iter().zip(field_types.into_iter()).collect();
		let layout = EncodedRowNamedLayout::new(fields);

		// Allocate and populate the new row
		let mut encoded_row = layout.allocate_row();
		layout.set_values(&mut encoded_row, &values);

		Ok(Row {
			number: row.number,
			encoded: encoded_row,
			layout,
		})
	}
}
