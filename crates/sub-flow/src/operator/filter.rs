// SPDX-License-Identifier: Apache-2.0
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
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::{
	Result,
	params::Params,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};

use crate::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

pub struct FilterOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	compiled_conditions: Vec<CompiledExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
}

impl FilterOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		conditions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
	) -> Self {
		let compile_ctx = CompileContext {
			symbols: &EMPTY_SYMBOL_TABLE,
		};
		let compiled_conditions: Vec<CompiledExpr> = conditions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile filter condition"))
			.collect();

		Self {
			parent,
			node,
			compiled_conditions,
			routines,
			runtime_context,
		}
	}

	fn evaluate(&self, columns: &Columns) -> Result<Vec<bool>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
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

		let mut mask = vec![true; row_count];

		for compiled_condition in &self.compiled_conditions {
			let result_col = compiled_condition.execute(&exec_ctx)?;

			for (row_idx, mask_val) in mask.iter_mut().enumerate() {
				if *mask_val {
					match result_col.data().get_value(row_idx) {
						Value::Boolean(true) => {}
						Value::Boolean(false) => *mask_val = false,
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

	fn filter_passing(&self, columns: &Columns, mask: &[bool]) -> Columns {
		let passing_indices: Vec<usize> =
			mask.iter().enumerate().filter(|&(_, pass)| *pass).map(|(idx, _)| idx).collect();

		if passing_indices.is_empty() {
			Columns::empty()
		} else {
			columns.extract_by_indices(&passing_indices)
		}
	}
}

impl Operator for FilterOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => self.apply_filter_insert(&post, &mut result)?,
				Diff::Update {
					pre,
					post,
				} => self.apply_filter_update(&pre, &post, &mut result)?,
				Diff::Remove {
					pre,
				} => self.apply_filter_remove(&pre, &mut result)?,
			}
		}

		Ok(Change::from_flow(self.node, change.version, result, change.changed_at))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		self.parent.pull(txn, rows)
	}
}

impl FilterOperator {
	#[inline]
	fn apply_filter_insert(&self, post: &Columns, result: &mut Vec<Diff>) -> Result<()> {
		let mask = self.evaluate(post)?;
		let passing = self.filter_passing(post, &mask);
		if !passing.is_empty() {
			result.push(Diff::insert(passing));
		}
		Ok(())
	}

	#[inline]
	fn apply_filter_remove(&self, pre: &Columns, result: &mut Vec<Diff>) -> Result<()> {
		let mask = self.evaluate(pre)?;
		let passing = self.filter_passing(pre, &mask);
		if !passing.is_empty() {
			result.push(Diff::remove(passing));
		}
		Ok(())
	}

	#[inline]
	fn apply_filter_update(&self, pre: &Columns, post: &Columns, result: &mut Vec<Diff>) -> Result<()> {
		let pre_mask = self.evaluate(pre)?;
		let post_mask = self.evaluate(post)?;

		let mut updated_idx = Vec::new();
		let mut inserted_idx = Vec::new();
		let mut removed_idx = Vec::new();

		let row_count = pre_mask.len().min(post_mask.len());
		for i in 0..row_count {
			match (pre_mask[i], post_mask[i]) {
				(true, true) => updated_idx.push(i),
				(false, true) => inserted_idx.push(i),
				(true, false) => removed_idx.push(i),
				(false, false) => {}
			}
		}

		if !updated_idx.is_empty() {
			result.push(Diff::update(
				pre.extract_by_indices(&updated_idx),
				post.extract_by_indices(&updated_idx),
			));
		}
		if !inserted_idx.is_empty() {
			result.push(Diff::insert(post.extract_by_indices(&inserted_idx)));
		}
		if !removed_idx.is_empty() {
			result.push(Diff::remove(pre.extract_by_indices(&removed_idx)));
		}
		Ok(())
	}
}
