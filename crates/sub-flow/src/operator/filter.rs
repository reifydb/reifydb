// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	internal_err,
	value::column::columns::Columns,
};
use reifydb_engine::{
	evaluate::compiled::{CompileContext, CompiledExpr, EvalContext, compile_expression},
	vm::stack::SymbolTable,
};
use reifydb_function::registry::Functions;
use reifydb_rql::expression::Expression;
use reifydb_runtime::clock::Clock;
use reifydb_type::{
	params::Params,
	value::{Value, row_number::RowNumber},
};

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());

pub struct FilterOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	compiled_conditions: Vec<CompiledExpr>,
	functions: Functions,
	clock: Clock,
}

impl FilterOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		conditions: Vec<Expression>,
		functions: Functions,
		clock: Clock,
	) -> Self {
		let compile_ctx = CompileContext {
			functions: &functions,
			symbol_table: &EMPTY_SYMBOL_TABLE,
		};
		let compiled_conditions: Vec<CompiledExpr> = conditions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile filter condition"))
			.collect();

		Self {
			parent,
			node,
			compiled_conditions,
			functions,
			clock,
		}
	}

	/// Evaluate filter on all rows in Columns
	/// Returns a boolean mask indicating which rows pass the filter
	fn evaluate(&self, columns: &Columns) -> reifydb_type::Result<Vec<bool>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let exec_ctx = EvalContext {
			target: None,
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
			symbol_table: &EMPTY_SYMBOL_TABLE,
			is_aggregate_context: false,
			functions: &self.functions,
			clock: &self.clock,
		};

		// Start with all rows passing
		let mut mask = vec![true; row_count];

		for compiled_condition in &self.compiled_conditions {
			let result_col = compiled_condition.execute(&exec_ctx)?;

			for row_idx in 0..row_count {
				if mask[row_idx] {
					match result_col.data().get_value(row_idx) {
						Value::Boolean(true) => {}
						Value::Boolean(false) => mask[row_idx] = false,
						result => {
							return internal_err!(
								"Filter condition did not evaluate to boolean, got: {:?}",
								result
							);
						}
					}
				}
			}
		}

		Ok(mask)
	}

	/// Filter Columns to only include rows that pass the filter
	fn filter_passing(&self, columns: &Columns, mask: &[bool]) -> Columns {
		let passing_indices: Vec<usize> =
			mask.iter().enumerate().filter(|&(_, pass)| *pass).map(|(idx, _)| idx).collect();

		if passing_indices.is_empty() {
			Columns::empty()
		} else {
			columns.extract_by_indices(&passing_indices)
		}
	}

	/// Filter Columns to only include rows that fail the filter
	fn filter_failing(&self, columns: &Columns, mask: &[bool]) -> Columns {
		let failing_indices: Vec<usize> =
			mask.iter().enumerate().filter(|&(_, pass)| !*pass).map(|(idx, _)| idx).collect();

		if failing_indices.is_empty() {
			Columns::empty()
		} else {
			columns.extract_by_indices(&failing_indices)
		}
	}
}

impl Operator for FilterOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> reifydb_type::Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => {
					let mask = self.evaluate(&post)?;
					let passing = self.filter_passing(&post, &mask);

					if !passing.is_empty() {
						result.push(Diff::Insert {
							post: passing,
						});
					}
				}
				Diff::Update {
					pre,
					post,
				} => {
					let mask = self.evaluate(&post)?;
					let passing = self.filter_passing(&post, &mask);
					let failing = self.filter_failing(&post, &mask);

					if !passing.is_empty() {
						let passing_indices: Vec<usize> = mask
							.iter()
							.enumerate()
							.filter(|&(_, pass)| *pass)
							.map(|(idx, _)| idx)
							.collect();
						let pre_passing = pre.extract_by_indices(&passing_indices);

						result.push(Diff::Update {
							pre: pre_passing,
							post: passing,
						});
					}

					if !failing.is_empty() {
						// Rows no longer match filter - emit removes for the pre values
						let failing_indices: Vec<usize> = mask
							.iter()
							.enumerate()
							.filter(|&(_, pass)| !*pass)
							.map(|(idx, _)| idx)
							.collect();
						let pre_failing = pre.extract_by_indices(&failing_indices);

						result.push(Diff::Remove {
							pre: pre_failing,
						});
					}
				}
				Diff::Remove {
					pre,
				} => {
					// Always pass through removes
					result.push(Diff::Remove {
						pre,
					});
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, result))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
