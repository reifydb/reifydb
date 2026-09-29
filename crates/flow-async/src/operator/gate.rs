// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use reifydb_codec::key::encode_u64_asc;
use reifydb_core::{
	expression::Expression,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey, KeyspaceId, OperatorStateKey},
	metrics::heap::{HeapSize, OperatorSample},
	value::batch::{empty_batch, take_rows},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_flow::context::FlowContext;
use reifydb_macro::operator_state;
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	value::{Value, column_view::ColumnView, row_number::RowNumber, system_columns::row_numbers},
};
use tracing::instrument;

use crate::operator::{
	HostOperator,
	host::HostContext,
	state_access::{get, put, remove},
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
		OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::GATE_VISIBILITY, encode_u64_asc(self.0.0))
	}
}

pub struct GateOperator {
	parent_schema: Option<SchemaRef>,
	operator: OperatorId,
	compiled_conditions: Vec<CompiledExpr>,
	routines: Routines,
	runtime_context: RuntimeContext,
	ctx: Arc<FlowContext>,
}

impl GateOperator {
	pub fn new(
		parent_schema: Option<SchemaRef>,
		operator: OperatorId,
		conditions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ctx: Arc<FlowContext>,
	) -> Result<Self> {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		let compiled_conditions: Vec<CompiledExpr> =
			conditions.iter().map(|e| compile_expression(&compile_ctx, e)).collect::<Result<Vec<_>>>()?;

		Ok(Self {
			parent_schema,
			operator,
			compiled_conditions,
			routines,
			runtime_context,
			ctx,
		})
	}

	pub(crate) fn output_schema(&self) -> Option<SchemaRef> {
		self.parent_schema.clone()
	}

	fn evaluate(&self, columns: &RecordBatch) -> Result<Vec<bool>> {
		let row_count = columns.num_rows();
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
			batch: empty_batch(),
			row_count: 1,
			target: None,
			take: None,
		};
		let exec_ctx = session.with_eval(columns.clone(), row_count);

		let mut mask = vec![true; row_count];

		for compiled_condition in &self.compiled_conditions {
			let result = compiled_condition.execute(&exec_ctx)?;
			let result_col = ColumnView::try_from(&result)?;

			for (row_idx, mask_val) in mask.iter_mut().enumerate() {
				if *mask_val {
					match result_col.get_value(row_idx) {
						Value::Boolean(true) => {}
						Value::Boolean(false) => *mask_val = false,
						_ => *mask_val = false,
					}
				}
			}
		}

		Ok(mask)
	}

	fn is_visible(&mut self, host: &mut dyn HostContext, rn: RowNumber) -> Result<bool> {
		Ok(get::<_, VisibilityMarker>(host, &VisibilityKey(rn))?.is_some())
	}

	fn mark_visible(&mut self, host: &mut dyn HostContext, rn: RowNumber) -> Result<()> {
		put(
			host,
			&VisibilityKey(rn),
			VisibilityMarker {
				visible: true,
			},
		)
	}

	fn mark_invisible(&mut self, host: &mut dyn HostContext, rn: RowNumber) -> Result<()> {
		remove(host, &VisibilityKey(rn))
	}
}

impl HostOperator for GateOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let mut result = Vec::new();

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_gate_insert(host, &post, &mut result)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_gate_update(host, pre, post, &mut result)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_gate_remove(host, pre, &mut result)?,
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result, change.changed_at))
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		self.output_schema()
	}
}

impl GateOperator {
	#[inline]
	#[instrument(name = "flow::operator::gate::insert", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_gate_insert(
		&mut self,
		host: &mut dyn HostContext,
		post: &RecordBatch,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let post_row_numbers = row_numbers(post)?;
		if post_row_numbers.is_empty() {
			let mask = self.evaluate(post)?;
			let passing_indices: Vec<usize> =
				mask.iter().enumerate().filter(|&(_, pass)| *pass).map(|(idx, _)| idx).collect();
			if !passing_indices.is_empty() {
				result.push(Diff::insert(take_rows(post, &passing_indices)?));
			}
			return Ok(());
		}

		let mask = self.evaluate(post)?;
		let mut passing_indices = Vec::new();
		for (i, &pass) in mask.iter().enumerate() {
			let rn = post_row_numbers[i];
			if pass {
				self.mark_visible(host, rn)?;
				passing_indices.push(i);
			}
		}
		if !passing_indices.is_empty() {
			result.push(Diff::insert(take_rows(post, &passing_indices)?));
		}
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::gate::update", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_gate_update(
		&mut self,
		host: &mut dyn HostContext,
		pre: RecordBatch,
		post: RecordBatch,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		if row_numbers(&post)?.is_empty() {
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

		for (i, (&rn, &mask_val)) in row_numbers(&post)?.iter().zip(mask.iter()).enumerate() {
			if self.is_visible(host, rn)? {
				update_indices.push(i);
			} else if mask_val {
				self.mark_visible(host, rn)?;
				insert_indices.push(i);
			}
		}

		if !update_indices.is_empty() {
			result.push(Diff::update(
				take_rows(&pre, &update_indices)?,
				take_rows(&post, &update_indices)?,
			));
		}
		if !insert_indices.is_empty() {
			result.push(Diff::insert(take_rows(&post, &insert_indices)?));
		}
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::gate::remove", level = "trace", skip_all, fields(rows = pre.num_rows()))]
	fn apply_gate_remove(
		&mut self,
		host: &mut dyn HostContext,
		pre: RecordBatch,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let pre_row_numbers = row_numbers(&pre)?;
		if pre_row_numbers.is_empty() {
			result.push(Diff::Remove {
				pre,
				origin: None,
			});
			return Ok(());
		}

		let mut remove_indices = Vec::new();
		for (i, &rn) in pre_row_numbers.iter().enumerate() {
			if self.is_visible(host, rn)? {
				self.mark_invisible(host, rn)?;
				remove_indices.push(i);
			}
		}

		if !remove_indices.is_empty() {
			result.push(Diff::remove(take_rows(&pre, &remove_indices)?));
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::key::operator::state::{
		GroupId, GroupStateKey, IntoGroupStateKey, KeyspaceId, OperatorStateKey, group_inner_range,
	};
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

	use super::VisibilityKey;

	#[test]
	fn a_visibility_key_lives_in_the_root_group_in_its_own_keyspace() {
		// b'G' (0x47) aliases a two-byte group-id varint, so the root group is what keeps a group reclaim from
		// deleting this key.
		let key = (&VisibilityKey(RowNumber(42))).into_group_state_key();

		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_bytes())
			.expect("a visibility marker must decode as a structured operator-state key");
		assert_eq!(group, GroupId::ROOT, "gate visibility must not live inside a reclaimable group");
		assert_eq!(keyspace, KeyspaceId::GATE_VISIBILITY);
		assert_eq!(suffix, 42u64.to_be_bytes().to_vec());
	}

	fn falls_inside(key: &GroupStateKey, range: &EncodedKeyRange) -> bool {
		let after_start = match &range.start {
			Bound::Included(s) => key.as_bytes() >= s.as_bytes(),
			Bound::Excluded(s) => key.as_bytes() > s.as_bytes(),
			Bound::Unbounded => true,
		};
		let before_end = match &range.end {
			Bound::Included(e) => key.as_bytes() <= e.as_bytes(),
			Bound::Excluded(e) => key.as_bytes() < e.as_bytes(),
			Bound::Unbounded => true,
		};
		after_start && before_end
	}

	#[test]
	fn a_visibility_key_sits_outside_every_reclaimable_groups_range() {
		// A marker caught inside any group's range dies when that group is reclaimed, so no hash may yield a
		// range that swallows it.
		let key = (&VisibilityKey(RowNumber(42))).into_group_state_key();

		for exponent in 0..128u32 {
			for hash in [1u128 << exponent, (1u128 << exponent).wrapping_sub(1), !(1u128 << exponent)] {
				let group = GroupId::hashed(Hash128(hash));
				assert!(
					!falls_inside(&key, &group_inner_range(group)),
					"a visibility marker must not fall inside the range of group {group}"
				);
			}
		}
	}

	#[test]
	fn a_visibility_key_sits_outside_every_window_groups_range() {
		// Window groups lead with a different field than hashed ones, so the marker must be unreachable from
		// every window id too.
		let key = (&VisibilityKey(RowNumber(42))).into_group_state_key();

		for exponent in 0..64u32 {
			let window_id = 1u64 << exponent;
			for partition in [Hash128(0), Hash128(1), Hash128(u128::MAX)] {
				let group = GroupId::window(partition, window_id);
				assert!(
					!falls_inside(&key, &group_inner_range(group)),
					"a visibility marker must not fall inside the range of group {group}"
				);
			}
		}
	}
}
