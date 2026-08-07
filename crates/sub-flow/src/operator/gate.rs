// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::UnsafeCell, sync::Arc};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::key::{
	decode_u64_asc, encode_u64_asc,
	encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	key::operator_group_state::{
		GroupId, GroupStateKey, IntoGroupStateKey, Keyspace, OperatorGroupStateKey, keyspace_inner_range,
	},
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
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::{Value, row_number::RowNumber},
};
use tracing::instrument;

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

impl IntoGroupStateKey for &VisibilityKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorGroupStateKey::inner_encoded(
			GroupId::NODE_SCOPE,
			Keyspace::GATE_VISIBILITY,
			encode_u64_asc(self.0.0),
		)
	}
}

fn decode_visibility_key(key: &EncodedKey) -> Option<VisibilityKey> {
	let (group, keyspace, suffix) = OperatorGroupStateKey::decode_inner(key.as_bytes())?;
	if group != GroupId::NODE_SCOPE || keyspace != Keyspace::GATE_VISIBILITY {
		return None;
	}
	let rn = decode_u64_asc(suffix.as_slice().try_into().ok()?);
	Some(VisibilityKey(RowNumber(rn)))
}

fn visibility_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::GATE_VISIBILITY)
}

struct GateState {
	visibility: StateCache<VisibilityKey, VisibilityMarker>,
	hydrated: bool,
}

impl GateState {
	fn new(budget: OperatorStateBudgetHandle) -> Self {
		Self {
			visibility: StateCache::new(budget),
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
		self.visibility.remove(store, &VisibilityKey(rn))
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
	operator: OperatorId,
	compiled_conditions: Vec<CompiledExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
	ctx: Arc<FlowContext>,
	state: UnsafeCell<GateState>,
}

impl GateOperator {
	pub fn new(
		parent: OperatorCell,
		operator: OperatorId,
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
			operator,
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
		// SAFETY: one actor drives this operator and apply never re-enters, so no other borrow
		// of the UnsafeCell is live while this &mut exists.
		unsafe { &mut *self.state.get() }
	}

	fn with_state<R>(
		&self,
		txn: &mut FlowTransaction,
		f: impl FnOnce(&mut FlowTransaction) -> Result<R>,
	) -> Result<R> {
		self.state_slot().hydrate_once(&mut OperatorStateStore::new(txn, self.operator))?;
		let out = f(txn)?;
		self.state_slot().flush(&mut OperatorStateStore::new(txn, self.operator))?;
		Ok(out)
	}

	fn is_visible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<bool> {
		self.state_slot().is_visible(&mut OperatorStateStore::new(txn, self.operator), rn)
	}

	fn mark_visible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<()> {
		self.state_slot().mark_visible(&mut OperatorStateStore::new(txn, self.operator), rn)
	}

	fn mark_invisible(&self, txn: &mut FlowTransaction, rn: RowNumber) -> Result<()> {
		self.state_slot().mark_invisible(&mut OperatorStateStore::new(txn, self.operator), rn)
	}
}

impl RawStatefulOperator for GateOperator {}

impl Operator for GateOperator {
	fn id(&self) -> OperatorId {
		self.operator
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

			Ok(Change::from_flow(self.operator, change.version, result, change.changed_at))
		})
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

impl GateOperator {
	#[inline]
	#[instrument(name = "flow::operator::gate::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_gate_insert(&self, txn: &mut FlowTransaction, post: &Columns, result: &mut Vec<Diff>) -> Result<()> {
		if post.row_numbers().is_empty() {
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
			let rn = post.row_numbers()[i];
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
	#[instrument(name = "flow::operator::gate::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_gate_update(
		&self,
		txn: &mut FlowTransaction,
		pre: Columns,
		post: Columns,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		if post.row_numbers().is_empty() {
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

		for (i, (&rn, &mask_val)) in post.row_numbers().iter().zip(mask.iter()).enumerate() {
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
	#[instrument(name = "flow::operator::gate::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn apply_gate_remove(&self, txn: &mut FlowTransaction, pre: Columns, result: &mut Vec<Diff>) -> Result<()> {
		if pre.row_numbers().is_empty() {
			result.push(Diff::Remove {
				pre,
				origin: None,
			});
			return Ok(());
		}

		let mut remove_indices = Vec::new();
		for i in 0..pre.row_numbers().len() {
			let rn = pre.row_numbers()[i];
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

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_core::key::operator_group_state::{
		GroupId, IntoGroupStateKey, Keyspace, OperatorGroupStateKey, group_inner_range,
	};
	use reifydb_value::value::row_number::RowNumber;

	use super::{VisibilityKey, decode_visibility_key, visibility_range};

	#[test]
	fn a_visibility_key_is_node_scoped_in_its_own_keyspace() {
		// A hand-rolled leading byte is indistinguishable from a group-id varint, and b'G' (0x47)
		// decodes into the two-byte tier, putting the key inside the range of a reachable group
		// id. Node scope is what keeps a reclaim of that group from range-deleting gate state.
		let key = (&VisibilityKey(RowNumber(42))).into_group_state_key();

		let (group, keyspace, suffix) = OperatorGroupStateKey::decode_inner(key.as_bytes())
			.expect("a visibility marker must decode as a structured operator-state key");
		assert_eq!(group, GroupId::NODE_SCOPE, "gate visibility must not live inside a reclaimable group");
		assert_eq!(keyspace, Keyspace::GATE_VISIBILITY);
		assert_eq!(suffix, 42u64.to_be_bytes().to_vec());
	}

	#[test]
	fn a_visibility_key_sits_outside_the_group_range_that_used_to_alias_it() {
		// Group 14591's inner prefix is exactly [0x47, 0x00], which every b'G'-tagged marker
		// below 2^56 shares. The tier boundaries either side are checked too, so a change to the
		// group encoding cannot quietly re-create the overlap.
		let key = (&VisibilityKey(RowNumber(42))).into_group_state_key();

		for group in [1u64, 127, 128, 14_336, 14_591, 16_383, 16_384] {
			let range = group_inner_range(GroupId(group));
			let start = match &range.start {
				Bound::Included(s) => key.as_bytes() >= s.as_bytes(),
				Bound::Excluded(s) => key.as_bytes() > s.as_bytes(),
				Bound::Unbounded => true,
			};
			let end = match &range.end {
				Bound::Included(e) => key.as_bytes() <= e.as_bytes(),
				Bound::Excluded(e) => key.as_bytes() < e.as_bytes(),
				Bound::Unbounded => true,
			};
			assert!(!(start && end), "a visibility marker must not fall inside the range of group {group}");
		}
	}

	#[test]
	fn the_visibility_range_round_trips_its_own_keys_and_admits_nothing_else() {
		let key = (&VisibilityKey(RowNumber(7))).into_group_state_key();
		assert_eq!(decode_visibility_key(key.as_encoded()).map(|k| k.0), Some(RowNumber(7)));

		let range = visibility_range();
		let start = match &range.start {
			Bound::Included(s) => key.as_bytes() >= s.as_bytes(),
			Bound::Excluded(s) => key.as_bytes() > s.as_bytes(),
			Bound::Unbounded => true,
		};
		let end = match &range.end {
			Bound::Included(e) => key.as_bytes() <= e.as_bytes(),
			Bound::Excluded(e) => key.as_bytes() < e.as_bytes(),
			Bound::Unbounded => true,
		};
		assert!(start && end, "hydration scans this range, so it must contain the keys the operator writes");

		let foreign = OperatorGroupStateKey::inner_encoded(
			GroupId::NODE_SCOPE,
			Keyspace::ACCUMULATOR,
			7u64.to_be_bytes(),
		);
		assert!(
			decode_visibility_key(foreign.as_encoded()).is_none(),
			"a neighbouring keyspace must not decode as visibility"
		);
	}
}
