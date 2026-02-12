// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	value::column::{Column, columns::Columns},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_function::registry::Functions;
use reifydb_rql::expression::Expression;
use reifydb_runtime::clock::Clock;
use reifydb_type::{fragment::Fragment, params::Params, value::row_number::RowNumber};

use crate::{Operator, operator::Operators, transaction::FlowTransaction};

// Static empty params instance for use in EvaluationContext
static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());

pub struct MapOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	expressions: Vec<Expression>,
	compiled_expressions: Vec<CompiledExpr>,
	functions: Functions,
	clock: Clock,
}

impl MapOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		expressions: Vec<Expression>,
		functions: Functions,
		clock: Clock,
	) -> Self {
		let compile_ctx = CompileContext {
			functions: &functions,
			symbol_table: &EMPTY_SYMBOL_TABLE,
		};
		let compiled_expressions: Vec<CompiledExpr> = expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>, _>>()
			.expect("Failed to compile expressions");

		Self {
			parent,
			node,
			expressions,
			compiled_expressions,
			functions,
			clock,
		}
	}

	/// Project all rows in Columns using expressions
	fn project(&self, columns: &Columns) -> reifydb_type::Result<Columns> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Columns::empty());
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
			arena: None,
		};

		let mut result_columns = Vec::with_capacity(self.expressions.len());

		for (i, compiled_expr) in self.compiled_expressions.iter().enumerate() {
			let evaluated_col = compiled_expr.execute(&exec_ctx)?;

			let expr = &self.expressions[i];
			let field_name = match expr {
				Expression::Alias(alias_expr) => alias_expr.alias.name().to_string(),
				Expression::Column(col_expr) => col_expr.0.name.text().to_string(),
				Expression::AccessSource(access_expr) => access_expr.column.name.text().to_string(),
				_ => expr.full_fragment_owned().text().to_string(),
			};

			let named_column = Column {
				name: Fragment::internal(field_name),
				data: evaluated_col.data().clone(),
			};

			result_columns.push(named_column);
		}

		let row_numbers = if columns.row_numbers.is_empty() {
			Vec::new()
		} else {
			columns.row_numbers.iter().cloned().collect()
		};

		Ok(Columns::with_row_numbers(result_columns, row_numbers))
	}
}

impl Operator for MapOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> reifydb_type::Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs.into_iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					let projected = match self.project(&post) {
						Ok(projected) => projected,
						Err(err) => {
							panic!("{:#?}", err)
						}
					};

					if !projected.is_empty() {
						result.push(Diff::Insert {
							post: projected,
						});
					}
				}
				Diff::Update {
					pre,
					post,
				} => {
					let projected_post = self.project(&post)?;
					let projected_pre = self.project(&pre)?;

					if !projected_post.is_empty() {
						result.push(Diff::Update {
							pre: projected_pre,
							post: projected_post,
						});
					}
				}
				Diff::Remove {
					pre,
				} => {
					let projected_pre = self.project(&pre)?;
					if !projected_pre.is_empty() {
						result.push(Diff::Remove {
							pre: projected_pre,
						});
					}
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, result))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
