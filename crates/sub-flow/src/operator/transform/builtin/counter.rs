// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey,
	flow::{FlowChange, FlowDiff},
	interface::{FlowNodeId, Transaction, expression::Expression},
	row::EncodedRow,
	util::CowVec,
	value::{
		columnar::{Column, ColumnData, ColumnQualified, Columns},
		container::NumberContainer,
	},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};

use crate::operator::{
	Operator,
	transform::{TransformOperator, TransformOperatorFactory, extract},
};

pub struct CounterOperator {
	node: FlowNodeId,
	increment: i64,
	column_name: String,
}

impl CounterOperator {
	fn get_counter_value<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
	) -> i64 {
		let empty_key = EncodedKey::new(Vec::new());
		let state_row =
			self.get(txn, &empty_key).unwrap_or_else(|_| {
				EncodedRow(CowVec::new(Vec::new()))
			});
		let state_bytes = state_row.as_ref();
		if state_bytes.len() >= 8 {
			i64::from_le_bytes(
				state_bytes[0..8].try_into().unwrap(),
			)
		} else {
			0
		}
	}

	fn set_counter_value<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		value: i64,
	) -> crate::Result<()> {
		let empty_key = EncodedKey::new(Vec::new());
		self.set(
			txn,
			&empty_key,
			EncodedRow(CowVec::new(value.to_le_bytes().to_vec())),
		)
	}
}

impl<T: Transaction> Operator<T> for CounterOperator {
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
					// Get current counter value
					let mut current =
						self.get_counter_value(txn);

					// Generate counter values for each row
					let row_count = after.row_count();
					let mut values =
						Vec::with_capacity(row_count);
					for _ in 0..row_count {
						current += self.increment;
						values.push(current);
					}

					// Save updated counter
					self.set_counter_value(txn, current)?;

					// Build output with counter column
					let mut all_columns: Vec<Column> =
						after.clone()
							.into_iter()
							.collect();
					all_columns.push(Column::ColumnQualified(ColumnQualified {
                        name: self.column_name.clone(),
                        data: ColumnData::Int8(NumberContainer::from_vec(values)),
                    }));
					let output_columns =
						Columns::new(all_columns);

					output.push(FlowDiff::Insert {
						source: *source,
						row_ids: row_ids.clone(),
						after: output_columns,
					});
				}

				FlowDiff::Update {
					source,
					row_ids,
					before,
					after,
				} => {
					// For updates, continue incrementing
					// the counter
					let mut current =
						self.get_counter_value(txn);

					let row_count = after.row_count();
					let mut values =
						Vec::with_capacity(row_count);
					for _ in 0..row_count {
						current += self.increment;
						values.push(current);
					}

					self.set_counter_value(txn, current)?;

					let mut all_columns: Vec<Column> =
						after.clone()
							.into_iter()
							.collect();
					all_columns.push(Column::ColumnQualified(ColumnQualified {
                        name: self.column_name.clone(),
                        data: ColumnData::Int8(NumberContainer::from_vec(values)),
                    }));
					let output_columns =
						Columns::new(all_columns);

					output.push(FlowDiff::Update {
						source: *source,
						row_ids: row_ids.clone(),
						before: before.clone(),
						after: output_columns,
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
						increment = extract::int(
							&alias_expr.expression,
						)?;
					}
					"column" | "name" => {
						column_name = extract::string(
							&alias_expr.expression,
						)?;
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
