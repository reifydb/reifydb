// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::{Expression, name::display_label};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, fragment::Fragment, params::Params, value::identity::IdentityId};

use crate::{Operator, operator::OperatorCell, transaction::FlowTransaction};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

pub struct ExtendOperator {
	parent: OperatorCell,
	node: FlowNodeId,
	expressions: Vec<Expression>,
	compiled_expressions: Vec<CompiledExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
}

impl ExtendOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		expressions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
	) -> Self {
		let compile_ctx = CompileContext {
			symbols: &EMPTY_SYMBOL_TABLE,
		};
		let compiled_expressions: Vec<CompiledExpr> = expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile expressions");

		Self {
			parent,
			node,
			expressions,
			compiled_expressions,
			routines,
			runtime_context,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}

	fn extend(&self, columns: &Columns) -> Result<Columns> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Columns::empty());
		}

		let session = EvalContext {
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		};
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut result_columns: Vec<ColumnWithName> =
			columns.iter().map(|col| ColumnWithName::new(col.name().clone(), col.data().clone())).collect();

		for (i, compiled_expr) in self.compiled_expressions.iter().enumerate() {
			let evaluated_col = compiled_expr.execute(&exec_ctx)?;

			let expr = &self.expressions[i];
			let field_name = display_label(expr).text().to_string();

			result_columns.push(ColumnWithName::new(
				Fragment::internal(field_name),
				evaluated_col.data().clone(),
			));
		}

		let row_numbers = if columns.row_numbers.is_empty() {
			Vec::new()
		} else {
			columns.row_numbers.iter().cloned().collect()
		};

		Ok(Columns::with_system_columns(
			result_columns,
			row_numbers,
			columns.created_at.to_vec(),
			columns.updated_at.to_vec(),
		))
	}
}

impl Operator for ExtendOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs.into_iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					let extended = match self.extend(&post) {
						Ok(extended) => extended,
						Err(err) => {
							panic!("{:#?}", err)
						}
					};

					if !extended.is_empty() {
						result.push(Diff::insert(extended));
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let extended_post = self.extend(&post)?;
					let extended_pre = self.extend(&pre)?;

					if !extended_post.is_empty() {
						result.push(Diff::update(extended_pre, extended_post));
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					let extended_pre = self.extend(&pre)?;
					if !extended_pre.is_empty() {
						result.push(Diff::remove(extended_pre));
					}
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, result, change.changed_at))
	}
}
