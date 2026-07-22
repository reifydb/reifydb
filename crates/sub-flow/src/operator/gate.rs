// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::UnsafeCell, ops::Bound, sync::Arc};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange, IntoEncodedKey};
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	key::flow_node_internal_state::FlowNodeInternalStateKey,
	metrics::heap::{HeapSize, OperatorSample},
	state::{budget::OperatorStateBudgetHandle, cache::StateCache, store::StateStore},
	value::column::columns::Columns,
};
use reifydb_engine::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_macro::operator_state;
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::{Value, row_number::RowNumber},
};

use crate::{
	context::FlowContext,
	operator::{OperatorCell, stateful::raw::RawStatefulOperator, store::OperatorStateStore},
};

#[operator_state]
#[derive(Clone, Default)]
struct VisibilityMarker {
	visible: bool,
}

impl HeapSize for VisibilityMarker {
	fn heap_size(&self) -> usize {
		0
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct VisibilityKey(RowNumber);

impl HeapSize for VisibilityKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &VisibilityKey {
	fn into_encoded_key(self) -> EncodedKey {
		let mut bytes = Vec::with_capacity(1 + 8);
		bytes.push(FlowNodeInternalStateKey::GATE_VISIBILITY_TAG);
		bytes.extend_from_slice(&self.0.0.to_be_bytes());
		EncodedKey::new(bytes)
	}
}

fn decode_visibility_key(key: &EncodedKey) -> Option<VisibilityKey> {
	let bytes = key.as_bytes();
	if bytes.first() != Some(&FlowNodeInternalStateKey::GATE_VISIBILITY_TAG) || bytes.len() != 1 + 8 {
		return None;
	}
	let rn = u64::from_be_bytes(bytes[1..9].try_into().ok()?);
	Some(VisibilityKey(RowNumber(rn)))
}

fn visibility_range() -> EncodedKeyRange {
	let tag = FlowNodeInternalStateKey::GATE_VISIBILITY_TAG;
	EncodedKeyRange::new(
		Bound::Included(EncodedKey::new(vec![tag])),
		Bound::Excluded(EncodedKey::new(vec![tag + 1])),
	)
}

struct GateState {
	visibility: StateCache<VisibilityKey, VisibilityMarker>,
	hydrated: bool,
}

impl GateState {
	fn new(budget: OperatorStateBudgetHandle) -> Self {
		Self {
			visibility: StateCache::new_internal(budget),
			hydrated: false,
		}
	}

	fn hydrate_once<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		self.visibility.hydrate(store, visibility_range(), decode_visibility_key)?;
		self.hydrated = true;
		Ok(())
	}

	fn flush<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		self.visibility.flush(store)
	}

	fn is_visible<S: StateStore>(&mut self, store: &mut S, rn: RowNumber) -> Result<bool> {
		Ok(self.visibility.read(store, &VisibilityKey(rn), |_| ())?.is_some())
	}

	fn mark_visible<S: StateStore>(&mut self, store: &mut S, rn: RowNumber) -> Result<()> {
		self.visibility.put(
			store,
			&VisibilityKey(rn),
			VisibilityMarker {
				visible: true,
			},
		)
	}

	fn mark_invisible<S: StateStore>(&mut self, store: &mut S, rn: RowNumber) -> Result<()> {
		self.visibility.drop(store, &VisibilityKey(rn))
	}

	fn sample(&self) -> Option<OperatorSample> {
		if !self.hydrated {
			return None;
		}
		Some(OperatorSample::with_memory(self.visibility.approximate_memory())
			.with_dirty_memory(self.visibility.dirty_memory())
			.with_membership(self.visibility.membership_memory())
			.with_completeness(self.visibility.completeness()))
	}
}

pub struct GateOperator {
	parent: OperatorCell,
	node: FlowNodeId,
	compiled_conditions: Vec<CompiledExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
	ctx: Arc<FlowContext>,
	state: UnsafeCell<GateState>,
}

impl GateOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		conditions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		state_budget: OperatorStateBudgetHandle,
		ctx: Arc<FlowContext>,
	) -> Self {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
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
			ctx,
			state: UnsafeCell::new(GateState::new(state_budget)),
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}

	fn evaluate(&self, columns: &Columns) -> Result<Vec<bool>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let session = EvalContext {
			params: &self.ctx.params,
			symbols: &self.ctx.symbols,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			arena: None,
			identity: self.ctx.identity,
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

	#[allow(clippy::mut_from_ref)]
	fn state_slot(&self) -> &mut GateState {
		// SAFETY: apply runs single-threaded per operator and never re-enters, so state_slot

		unsafe { &mut *self.state.get() }
	}

	fn with_state<R>(
		&self,
		txn: &mut FlowTransaction,
		f: impl FnOnce(&mut FlowTransaction) -> Result<R>,
	) -> Result<R> {
		self.state_slot().hydrate_once(&mut OperatorStateStore::new(txn, self.node))?;
		let out = f(txn)?;
		self.state_slot().flush(&mut OperatorStateStore::new(txn, self.node))?;
		Ok(out)
	}

	fn is_visible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<bool> {
		self.state_slot().is_visible(&mut OperatorStateStore::new(txn, self.node), rn)
	}

	fn mark_visible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<()> {
		self.state_slot().mark_visible(&mut OperatorStateStore::new(txn, self.node), rn)
	}

	fn mark_invisible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<()> {
		self.state_slot().mark_invisible(&mut OperatorStateStore::new(txn, self.node), rn)
	}
}

impl RawStatefulOperator for GateOperator {}

impl Operator for GateOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.state_slot().sample()
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.with_state(txn, |txn| {
			let mut result = Vec::new();

			for diff in change.diffs {
				match diff {
					Diff::Insert {
						post,
						..
					} => self.apply_gate_insert(txn, &post, &mut result)?,
					Diff::Update {
						pre,
						post,
						..
					} => self.apply_gate_update(txn, pre, post, &mut result)?,
					Diff::Remove {
						pre,
						..
					} => self.apply_gate_remove(txn, pre, &mut result)?,
				}
			}

			Ok(Change::from_flow(self.node, change.version, result, change.changed_at))
		})
	}
}

impl GateOperator {
	#[inline]
	fn apply_gate_insert(&self, txn: &mut FlowTransaction, post: &Columns, result: &mut Vec<Diff>) -> Result<()> {
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
		pre: Columns,
		post: Columns,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		if post.row_numbers.is_empty() {
			result.push(Diff::Update {
				pre,
				post,
				origin: None,
			});
			return Ok(());
		}

		let mask = self.evaluate(&post)?;
		let mut update_indices = Vec::new();
		let mut insert_indices = Vec::new();

		for (i, (&rn, &mask_val)) in post.row_numbers.iter().zip(mask.iter()).enumerate() {
			if self.is_visible(txn, rn)? {
				update_indices.push(i);
			} else if mask_val {
				self.mark_visible(txn, rn)?;
				insert_indices.push(i);
			}
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
	fn apply_gate_remove(&self, txn: &mut FlowTransaction, pre: Columns, result: &mut Vec<Diff>) -> Result<()> {
		if pre.row_numbers.is_empty() {
			result.push(Diff::Remove {
				pre,
				origin: None,
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
		}

		if !remove_indices.is_empty() {
			result.push(Diff::remove(pre.extract_by_indices(&remove_indices)));
		}
		Ok(())
	}
}
