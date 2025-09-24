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
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					result.push(FlowDiff::Insert {
						post: self.project_row(&post, evaluator)?,
					});
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					result.push(FlowDiff::Update {
						pre,
						post: self.project_row(&post, evaluator)?,
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
			let value = evaluator.evaluate(&ctx, expr)?;

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
