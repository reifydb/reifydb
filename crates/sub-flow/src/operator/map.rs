// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{CommitVersion, Row, interface::FlowNodeId, value::encoded::EncodedValuesNamedLayout};
use reifydb_engine::{RowEvaluationContext, StandardCommandTransaction, StandardRowEvaluator};
use reifydb_rql::expression::Expression;
use reifydb_type::{Params, RowNumber, Type};

use crate::{
	Operator,
	flow::{FlowChange, FlowDiff},
	operator::Operators,
};

// Static empty params instance for use in RowEvaluationContext
static EMPTY_PARAMS: Params = Params::None;

pub struct MapOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	expressions: Vec<Expression<'static>>,
}

impl MapOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, expressions: Vec<Expression<'static>>) -> Self {
		Self {
			parent,
			node,
			expressions,
		}
	}
}

impl Operator for MapOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction,
		change: FlowChange,
		evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		let mut result = Vec::new();

		for diff in change.diffs.into_iter() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let projected = match self.project_row(&post, evaluator) {
						Ok(projected) => projected,
						Err(err) => {
							panic!("{:#?}", err)
						}
					};

					result.push(FlowDiff::Insert {
						post: projected,
					});
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					let projected = self.project_row(&post, evaluator)?;
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

		Ok(FlowChange::internal(self.node, change.version, result))
	}

	fn get_rows(
		&self,
		txn: &mut StandardCommandTransaction,
		rows: &[RowNumber],
		version: CommitVersion,
	) -> crate::Result<Vec<Option<Row>>> {
		self.parent.get_rows(txn, rows, version)
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

						// Find the column by name in the encoded
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

							// Find the column by name in the encoded
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
		let layout = EncodedValuesNamedLayout::new(fields);

		// Allocate and populate the new encoded
		let mut encoded_row = layout.allocate_row();
		layout.set_values(&mut encoded_row, &values);

		Ok(Row {
			number: row.number,
			encoded: encoded_row,
			layout,
		})
	}
}
