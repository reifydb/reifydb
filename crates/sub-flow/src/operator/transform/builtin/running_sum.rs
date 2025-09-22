// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey,
	flow::{FlowChange, FlowDiff},
	interface::{EvaluationContext, Evaluator, FlowNodeId, Transaction, expression::Expression},
	util::CowVec,
	value::{
		column::{Column, ColumnComputed, ColumnData, Columns},
		container::NumberContainer,
		row::EncodedRow,
	},
};
use reifydb_engine::{StandardCommandTransaction, StandardEvaluator};
use reifydb_type::{Fragment, Params};

use crate::operator::{
	Operator,
	transform::{TransformOperator, TransformOperatorFactory, extract, stateful::RawStatefulOperator},
};

pub struct RunningSumOperator {
	node: FlowNodeId,
	input_expression: Expression<'static>,
	column_name: String,
}

impl RunningSumOperator {
	fn parse_state(&self, bytes: &[u8]) -> f64 {
		if bytes.len() >= 8 {
			f64::from_le_bytes(bytes[0..8].try_into().unwrap())
		} else {
			0.0
		}
	}

	fn encode_state(&self, sum: f64) -> Vec<u8> {
		sum.to_le_bytes().to_vec()
	}
}

impl<T: Transaction> Operator<T> for RunningSumOperator {
	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
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
					// Evaluate input expression
					let empty_params = Params::None;
					let eval_ctx = EvaluationContext {
						target: None,
						columns: after.clone(),
						row_count: after.row_count(),
						take: None,
						params: &empty_params,
					};

					let input_column = evaluator.evaluate(&eval_ctx, &self.input_expression)?;

					// Get current sum
					let empty_key = EncodedKey::new(Vec::new());
					let mut sum = match self.state_get(txn, &empty_key)? {
						Some(state_row) => self.parse_state(state_row.as_ref()),
						None => 0.0,
					};

					let row_count = after.row_count();
					let mut sums = Vec::with_capacity(row_count);

					// Process values
					match input_column.data() {
						ColumnData::Float8(container) => {
							for val in container.data().iter() {
								sum += val;
								sums.push(sum);
							}
						}
						ColumnData::Int8(container) => {
							for val in container.data().iter() {
								sum += *val as f64;
								sums.push(sum);
							}
						}
						_ => panic!("running_sum requires numeric input"),
					}

					// Save updated sum
					let empty_key = EncodedKey::new(Vec::new());
					self.state_set(
						txn,
						&empty_key,
						EncodedRow(CowVec::new(self.encode_state(sum))),
					)?;

					// Build output
					let mut all_columns: Vec<Column> = after.clone().into_iter().collect();
					all_columns.push(Column::Computed(ColumnComputed {
						name: Fragment::owned_internal(self.column_name.clone()),
						data: ColumnData::Float8(NumberContainer::from_vec(sums)),
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
					// For updates, process the new values
					// Evaluate input expression
					let empty_params = Params::None;
					let eval_ctx = EvaluationContext {
						target: None,
						columns: after.clone(),
						row_count: after.row_count(),
						take: None,
						params: &empty_params,
					};

					let input_column = evaluator.evaluate(&eval_ctx, &self.input_expression)?;

					// Get current sum
					let empty_key = EncodedKey::new(Vec::new());
					let mut sum = match self.state_get(txn, &empty_key)? {
						Some(state_row) => self.parse_state(state_row.as_ref()),
						None => 0.0,
					};

					let row_count = after.row_count();
					let mut sums = Vec::with_capacity(row_count);

					// Process values
					match input_column.data() {
						ColumnData::Float8(container) => {
							for val in container.data().iter() {
								sum += val;
								sums.push(sum);
							}
						}
						ColumnData::Int8(container) => {
							for val in container.data().iter() {
								sum += *val as f64;
								sums.push(sum);
							}
						}
						_ => panic!("running_sum requires numeric input"),
					}

					// Save updated sum
					let empty_key = EncodedKey::new(Vec::new());
					self.state_set(
						txn,
						&empty_key,
						EncodedRow(CowVec::new(self.encode_state(sum))),
					)?;

					// Build output
					let mut all_columns: Vec<Column> = after.clone().into_iter().collect();
					all_columns.push(Column::Computed(ColumnComputed {
						name: Fragment::owned_internal(self.column_name.clone()),
						data: ColumnData::Float8(NumberContainer::from_vec(sums)),
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
					output.push(diff.clone());
				}
			}
		}

		Ok(FlowChange::new(output))
	}
}

impl<T: Transaction> TransformOperator<T> for RunningSumOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}
}

impl<T: Transaction> RawStatefulOperator<T> for RunningSumOperator {}

impl<T: Transaction> TransformOperatorFactory<T> for RunningSumOperator {
	fn create_from_expressions(
		node: FlowNodeId,
		expressions: &[Expression<'static>],
	) -> crate::Result<Box<dyn Operator<T>>> {
		let mut input_expression = None;
		let mut column_name = "running_sum".to_string();

		for expr in expressions {
			if let Expression::Alias(alias_expr) = expr {
				match alias_expr.alias.to_string().as_str() {
					"input" | "value" => {
						input_expression = Some(alias_expr.expression.clone());
					}
					"column" | "name" => {
						column_name = extract::string(&alias_expr.expression)?;
					}
					_ => {}
				}
			} else if input_expression.is_none() {
				// If it's not an alias expression and we don't
				// have an input yet, treat it as the input
				// parameter
				input_expression = Some(Box::new(expr.clone()));
			}
		}

		let input_expression =
			*input_expression.unwrap_or_else(|| panic!("running_sum requires 'input' parameter"));

		Ok(Box::new(RunningSumOperator {
			node,
			input_expression,
			column_name,
		}))
	}
}
