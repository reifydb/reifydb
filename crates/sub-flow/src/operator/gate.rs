// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
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
	util::cowvec::CowVec,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};

use crate::{
	operator::{Operator, Operators, stateful::raw::RawStatefulOperator},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

/// A sentinel value stored to mark a row as "visible" (latch open).
static VISIBLE_MARKER: LazyLock<EncodedRow> = LazyLock::new(|| EncodedRow(CowVec::new(vec![1])));

pub struct GateOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	compiled_conditions: Vec<CompiledExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
}

impl GateOperator {
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
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile gate condition"))
			.collect();

		Self {
			parent,
			node,
			compiled_conditions,
			routines,
			runtime_context,
		}
	}

	/// Evaluate conditions on all rows in Columns.
	/// Returns a boolean mask indicating which rows pass the conditions.
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
						_ => *mask_val = false,
					}
				}
			}
		}

		Ok(mask)
	}

	fn row_number_key(rn: RowNumber) -> EncodedKey {
		EncodedKey::new(rn.0.to_be_bytes().to_vec())
	}

	fn is_visible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<bool> {
		Ok(self.state_get(txn, &Self::row_number_key(rn))?.is_some())
	}

	fn mark_visible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<()> {
		self.state_set(txn, &Self::row_number_key(rn), VISIBLE_MARKER.clone())
	}

	fn mark_invisible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<()> {
		self.state_remove(txn, &Self::row_number_key(rn))
	}
}

impl RawStatefulOperator for GateOperator {}

impl Operator for GateOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => self.apply_gate_insert(txn, &post, &mut result)?,
				Diff::Update {
					pre,
					post,
				} => self.apply_gate_update(txn, pre, post, &mut result)?,
				Diff::Remove {
					pre,
				} => self.apply_gate_remove(txn, pre, &mut result)?,
			}
		}

		Ok(Change::from_flow(self.node, change.version, result, change.changed_at))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		self.parent.pull(txn, rows)
	}
}

impl GateOperator {
	#[inline]
	fn apply_gate_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Arc<Columns>,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		// Without row_numbers there is no per-row state; behave as a pure filter.
		if post.row_numbers.is_empty() {
			let mask = self.evaluate(post)?;
			let passing_indices: Vec<usize> =
				mask.iter().enumerate().filter(|&(_, pass)| *pass).map(|(idx, _)| idx).collect();
			if !passing_indices.is_empty() {
				result.push(Diff::insert(post.extract_by_indices(&passing_indices)));
			}
			return Ok(());
		}

		let mask = self.evaluate(post)?;
		let mut passing_indices = Vec::new();
		for (i, &pass) in mask.iter().enumerate() {
			let rn = post.row_numbers[i];
			if pass {
				self.mark_visible(txn, rn)?;
				passing_indices.push(i);
			}
			// not pass: drop, latch stays closed
		}
		if !passing_indices.is_empty() {
			result.push(Diff::insert(post.extract_by_indices(&passing_indices)));
		}
		Ok(())
	}

	#[inline]
	fn apply_gate_update(
		&self,
		txn: &mut FlowTransaction,
		pre: Arc<Columns>,
		post: Arc<Columns>,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		// Without row_numbers there is no per-row state to consult; treat the
		// row as already-visible and pass the Update through unchanged.
		if post.row_numbers.is_empty() {
			result.push(Diff::Update {
				pre,
				post,
			});
			return Ok(());
		}

		let mask = self.evaluate(&post)?;
		let mut update_indices = Vec::new();
		let mut insert_indices = Vec::new();

		for (i, (&rn, &mask_val)) in post.row_numbers.iter().zip(mask.iter()).enumerate() {
			if self.is_visible(txn, rn)? {
				// latch already open: forward Update unconditionally
				update_indices.push(i);
			} else if mask_val {
				// latch closed but condition now passes: open and emit as Insert
				self.mark_visible(txn, rn)?;
				insert_indices.push(i);
			}
			// closed and still failing: drop
		}

		if !update_indices.is_empty() {
			result.push(Diff::update(
				pre.extract_by_indices(&update_indices),
				post.extract_by_indices(&update_indices),
			));
		}
		if !insert_indices.is_empty() {
			result.push(Diff::insert(post.extract_by_indices(&insert_indices)));
		}
		Ok(())
	}

	#[inline]
	fn apply_gate_remove(
		&self,
		txn: &mut FlowTransaction,
		pre: Arc<Columns>,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		// Without row_numbers there is no per-row state; pass through.
		if pre.row_numbers.is_empty() {
			result.push(Diff::Remove {
				pre,
			});
			return Ok(());
		}

		let mut remove_indices = Vec::new();
		for i in 0..pre.row_numbers.len() {
			let rn = pre.row_numbers[i];
			if self.is_visible(txn, rn)? {
				self.mark_invisible(txn, rn)?;
				remove_indices.push(i);
			}
			// never visible: drop
		}

		if !remove_indices.is_empty() {
			result.push(Diff::remove(pre.extract_by_indices(&remove_indices)));
		}
		Ok(())
	}
}
