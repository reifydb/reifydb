// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	flow::{FlowChange, FlowDiff},
	interface::{FlowNodeId, Transaction, expression::Expression},
	row::EncodedRowLayout,
	value::{
		columnar::{Column, ColumnData, ColumnQualified, Columns},
		container::NumberContainer,
	},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_type::Type;

use crate::{
	operator::{
		Operator,
		transform::{TransformOperator, TransformOperatorFactory, extract, stateful::SingleStateful},
	},
	stateful::RawStatefulOperator,
};

pub struct CounterOperator {
	node: FlowNodeId,
	increment: i64,
	column_name: String,
}

impl<T: Transaction> Operator<T> for CounterOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardEvaluator,
	) -> crate::Result<FlowChange> {
		let mut output = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					source,
					rows: row_ids,
					post: after,
				} => {
					// Update counter using row-based state
					let row_count = after.row_count();
					let mut values = Vec::with_capacity(row_count);

					// Update state and generate counter values
					self.update_state(txn, |layout, row| {
						let mut current = layout.get_i64(row, 0);
						for _ in 0..row_count {
							current += self.increment;
							values.push(current);
						}
						layout.set_i64(row, 0, current);
						Ok(())
					})?;

					// Build output with counter column
					let mut all_columns: Vec<Column> = after.clone().into_iter().collect();
					all_columns.push(Column::ColumnQualified(ColumnQualified {
						name: self.column_name.clone(),
						data: ColumnData::Int8(NumberContainer::from_vec(values)),
					}));
					let output_columns = Columns::new(all_columns);

					output.push(FlowDiff::Insert {
						source,
						rows: row_ids.clone(),
						post: output_columns,
					});
				}

				FlowDiff::Update {
					source,
					rows: row_ids,
					pre: before,
					post: after,
				} => {
					// For updates, continue incrementing the counter
					let row_count = after.row_count();
					let mut values = Vec::with_capacity(row_count);

					// Update state and generate counter values
					self.update_state(txn, |layout, row| {
						let mut current = layout.get_i64(row, 0);
						for _ in 0..row_count {
							current += self.increment;
							values.push(current);
						}
						layout.set_i64(row, 0, current);
						Ok(())
					})?;

					let mut all_columns: Vec<Column> = after.clone().into_iter().collect();
					all_columns.push(Column::ColumnQualified(ColumnQualified {
						name: self.column_name.clone(),
						data: ColumnData::Int8(NumberContainer::from_vec(values)),
					}));
					let output_columns = Columns::new(all_columns);

					output.push(FlowDiff::Update {
						source,
						rows: row_ids.clone(),
						pre: before.clone(),
						post: output_columns,
					});
				}

				FlowDiff::Remove {
					..
				} => {
					// Pass through removes unchanged
					output.push(diff.clone());
				}
			}
		}

		Ok(FlowChange::new(output))
	}
}

impl<T: Transaction> TransformOperator<T> for CounterOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}
}

impl<T: Transaction> RawStatefulOperator<T> for CounterOperator {}

impl<T: Transaction> SingleStateful<T> for CounterOperator {
	fn layout(&self) -> EncodedRowLayout {
		// Counter state: [count: Int8]
		static SCHEMA: &[Type] = &[Type::Int8];
		EncodedRowLayout::new(SCHEMA)
	}
}

impl<T: Transaction> TransformOperatorFactory<T> for CounterOperator {
	fn create_from_expressions(
		node: FlowNodeId,
		expressions: &[Expression<'static>],
	) -> crate::Result<Box<dyn Operator<T>>> {
		let mut increment = 1i64;
		let mut column_name = "row_number".to_string();

		// Parse alias expressions for configuration
		for expr in expressions {
			if let Expression::Alias(alias_expr) = expr {
				match alias_expr.alias.to_string().as_str() {
					"increment" => {
						increment = extract::int(&alias_expr.expression)?;
					}
					"column" | "name" => {
						column_name = extract::string(&alias_expr.expression)?;
					}
					_ => {
						// Ignore unknown parameters
					}
				}
			}
		}

		Ok(Box::new(CounterOperator {
			node,
			increment,
			column_name,
		}))
	}
}
