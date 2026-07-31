// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use postcard::to_extend;
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	encoded::shape::RowShape,
	key::{encoded::EncodedKey, serializer::KeySerializer},
};
use reifydb_core::{
	common::JoinType,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin, Diff},
	},
	key::operator_state::{GroupId, GroupSet, Keyspace},
	metrics::heap::OperatorSample,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::executor::Executor,
};
use reifydb_flow::{
	operator::{Operator, Reclaimable},
	transaction::FlowTransaction,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::{Hash128, xxh3_128},
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber, value_type::ValueType},
};

use super::{
	column::JoinedColumnsBuilder,
	snapshot::{SnapshotLedger, snapshot_ledger_keyspaces},
	state::{JoinMembership, JoinSide, JoinState},
	strategy::{JoinContext, JoinStrategy, UpdateKeys},
};
use crate::{
	context::FlowContext,
	error::{FlowGraphError, FlowStateError},
	operator::stateful::{raw::RawStatefulOperator, single::SingleStateful},
};

const CAPABILITIES: &[OperatorCapability] = &[
	OperatorCapability::Insert,
	OperatorCapability::Update,
	OperatorCapability::Delete,
	OperatorCapability::Reclaim,
];

fn group_by_key(keys: &[Option<Hash128>]) -> (Vec<Hash128>, HashMap<Hash128, Vec<usize>>, Vec<usize>) {
	let mut order: Vec<Hash128> = Vec::new();
	let mut groups: HashMap<Hash128, Vec<usize>> = HashMap::new();
	let mut undefined: Vec<usize> = Vec::new();
	for (row_idx, key) in keys.iter().enumerate() {
		match key {
			Some(key_hash) => {
				groups.entry(*key_hash)
					.or_insert_with(|| {
						order.push(*key_hash);
						Vec::new()
					})
					.push(row_idx);
			}
			None => undefined.push(row_idx),
		}
	}
	(order, groups, undefined)
}

#[cfg(test)]
mod group_by_key_tests {
	use super::*;

	fn h(v: u128) -> Hash128 {
		Hash128(v)
	}

	#[test]
	fn groups_duplicate_keys_and_preserves_first_occurrence_order() {
		// Latest-mode dispatch iterates `order` to issue one strategy call per key. The order must be
		// first-occurrence so that, combined with latest's left-row-number reuse, output identity is
		// stable; and every input index must land in exactly one group with none dropped or duplicated.
		let keys = vec![Some(h(0xA)), Some(h(0xB)), Some(h(0xA)), Some(h(0xC)), Some(h(0xB))];
		let (order, groups, undefined) = group_by_key(&keys);

		assert_eq!(order, vec![h(0xA), h(0xB), h(0xC)], "keys must appear in first-occurrence order");
		assert_eq!(groups[&h(0xA)], vec![0, 2], "indices within a group keep input order");
		assert_eq!(groups[&h(0xB)], vec![1, 4]);
		assert_eq!(groups[&h(0xC)], vec![3]);
		assert!(undefined.is_empty());

		let regrouped: usize = order.iter().map(|k| groups[k].len()).sum();
		assert_eq!(regrouped, 5, "every defined row is grouped exactly once");
	}

	#[test]
	fn routes_none_keys_to_undefined_without_grouping_them() {
		// None-key rows take the per-row undefined path (they never probe); they must not create a
		// group keyed by some sentinel hash, or an undefined row would be mis-joined.
		let keys = vec![None, Some(h(0xA)), None, Some(h(0xA))];
		let (order, groups, undefined) = group_by_key(&keys);

		assert_eq!(order, vec![h(0xA)]);
		assert_eq!(groups[&h(0xA)], vec![1, 3]);
		assert_eq!(undefined, vec![0, 2], "none-key rows are collected in input order, separately");
	}

	#[test]
	fn empty_input_yields_empty_partitions() {
		let (order, groups, undefined) = group_by_key(&[]);
		assert!(order.is_empty());
		assert!(groups.is_empty());
		assert!(undefined.is_empty());
	}
}

pub struct JoinSideConfig {
	pub node: FlowNodeId,
	pub exprs: Vec<Expression>,
	pub schema: Columns,
}

pub struct JoinOperator {
	node: FlowNodeId,
	strategy: JoinStrategy,
	left_node: FlowNodeId,
	right_node: FlowNodeId,
	compiled_left_exprs: Vec<CompiledExpr>,
	compiled_right_exprs: Vec<CompiledExpr>,
	alias: Option<String>,
	shape: RowShape,
	right_schema: Columns,
	routines: Routines,
	runtime_context: RuntimeContext,
	pub(crate) snapshot: bool,
	natural: bool,
	pub(crate) latest: bool,
	left_ttl: Option<Duration>,
	right_ttl: Option<Duration>,
	membership: Arc<JoinMembership>,
	ctx: Arc<FlowContext>,
}

impl JoinOperator {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		left: JoinSideConfig,
		right: JoinSideConfig,
		node: FlowNodeId,
		join_type: JoinType,
		alias: Option<String>,
		executor: Executor,
		snapshot: bool,
		natural: bool,
		latest: bool,
		left_ttl: Option<Duration>,
		right_ttl: Option<Duration>,
		ctx: Arc<FlowContext>,
	) -> Self {
		let left_node = left.node;
		let right_node = right.node;
		let left_exprs = left.exprs;
		let right_exprs = right.exprs;
		let right_schema = right.schema;
		let strategy = JoinStrategy::from(join_type, latest);
		let shape = Self::state_shape();

		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};

		let compiled_left_exprs: Vec<CompiledExpr> = left_exprs
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile left expressions");

		let compiled_right_exprs: Vec<CompiledExpr> = right_exprs
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile right expressions");

		let routines = executor.routines.clone();
		let runtime_context = executor.runtime_context.clone();

		Self {
			node,
			strategy,
			left_node,
			right_node,
			compiled_left_exprs,
			compiled_right_exprs,
			alias,
			shape,
			right_schema,
			routines,
			runtime_context,
			snapshot,
			natural,
			latest,
			left_ttl,
			right_ttl,
			membership: Arc::new(JoinMembership::new()),
			ctx,
		}
	}

	fn state_shape() -> RowShape {
		RowShape::operator_state()
	}

	#[cfg(test)]
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new_for_state_tests(
		node: FlowNodeId,
		left_ttl: Option<Duration>,
		right_ttl: Option<Duration>,
		routines: Routines,
		runtime_context: RuntimeContext,
	) -> Self {
		Self {
			node,
			strategy: JoinStrategy::from(JoinType::Inner, false),
			left_node: FlowNodeId(0),
			right_node: FlowNodeId(0),
			compiled_left_exprs: Vec::new(),
			compiled_right_exprs: Vec::new(),
			alias: None,
			shape: Self::state_shape(),
			right_schema: Columns::empty(),
			routines,
			runtime_context,
			snapshot: true,
			natural: false,
			latest: false,
			left_ttl,
			right_ttl,
			membership: Arc::new(JoinMembership::new()),
			ctx: Arc::new(FlowContext::default()),
		}
	}

	pub(crate) fn snapshot_ledger(&self) -> SnapshotLedger {
		SnapshotLedger::new(self.node)
	}

	pub(crate) fn compute_join_keys(
		&self,
		columns: &Columns,
		compiled_exprs: &[CompiledExpr],
	) -> Result<Vec<Option<Hash128>>> {
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

		let mut expr_columns = Vec::with_capacity(compiled_exprs.len());
		for compiled_expr in compiled_exprs.iter() {
			let col: ColumnWithName = if let Some(col_name) = compiled_expr.access_column_name() {
				columns.column(col_name)
					.map(|c| ColumnWithName::new(c.name().clone(), c.data().clone()))
					.unwrap_or_else(|| {
						ColumnWithName::undefined_typed(col_name, ValueType::Boolean, row_count)
					})
			} else {
				compiled_expr.execute(&exec_ctx)?
			};
			expr_columns.push(col);
		}

		let mut hashes = Vec::with_capacity(row_count);
		let mut buf: Vec<u8> = Vec::with_capacity(256);
		for row_idx in 0..row_count {
			buf.clear();
			let mut has_undefined = false;

			for col in &expr_columns {
				let value = col.data().get_value(row_idx);

				if matches!(value, Value::None { .. }) {
					has_undefined = true;
					break;
				}

				buf = to_extend(&value, buf).map_err(|e| {
					Error::from(FlowStateError::Encode {
						state: "value for hash",
						cause: e.to_string(),
					})
				})?;
			}

			if has_undefined {
				hashes.push(None);
			} else {
				hashes.push(Some(xxh3_128(&buf)));
			}
		}

		Ok(hashes)
	}

	pub(crate) fn unmatched_left_columns(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_idx: usize,
	) -> Result<Columns> {
		let left_row_number = left.row_numbers()[left_idx];

		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_row_number.0);
		let composite_key = serializer.finish();

		let (result_row_number, _is_new) =
			txn.get_or_create_row_number(self.node, GroupId::NODE_SCOPE, &composite_key)?;

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		Ok(builder.unmatched_left(result_row_number, left, left_idx, &self.right_schema))
	}

	pub(crate) fn unmatched_left_columns_batch(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_indices: &[usize],
	) -> Result<Columns> {
		if left_indices.is_empty() {
			return Ok(Columns::empty());
		}

		let composite_keys: Vec<EncodedKey> = left_indices
			.iter()
			.map(|&idx| {
				let left_row_number = left.row_numbers()[idx];
				let mut serializer = KeySerializer::new();
				serializer.extend_u8(b'L');
				serializer.extend_u64(left_row_number.0);
				serializer.finish()
			})
			.collect();

		let row_numbers_with_flags =
			txn.get_or_create_row_numbers(self.node, GroupId::NODE_SCOPE, &composite_keys)?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		Ok(builder.unmatched_left_batch(&row_numbers, left, left_indices, &self.right_schema))
	}

	pub(crate) fn cleanup_left_row_joins(&self, txn: &mut FlowTransaction, left_number: u64) -> Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		txn.remove_row_numbers_by_prefix(self.node, GroupId::NODE_SCOPE, &prefix)
	}

	fn make_composite_key(left_num: RowNumber, right_num: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_num.0);
		serializer.extend_u64(right_num.0);
		serializer.finish()
	}

	pub(crate) fn join_columns_one_to_many(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_idx: usize,
		right: &Columns,
	) -> Result<Columns> {
		let right_count = right.row_count();
		if right_count == 0 {
			return Ok(Columns::empty());
		}

		let left_row_number = left.row_numbers()[left_idx];

		let composite_keys: Vec<EncodedKey> = (0..right_count)
			.map(|right_idx| {
				let right_row_number = right.row_numbers()[right_idx];
				Self::make_composite_key(left_row_number, right_row_number)
			})
			.collect();

		let row_numbers_with_flags =
			txn.get_or_create_row_numbers(self.node, GroupId::NODE_SCOPE, &composite_keys)?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		Ok(builder.join_one_to_many(&row_numbers, left, left_idx, right))
	}

	pub(crate) fn join_columns_many_to_one(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		right: &Columns,
		right_idx: usize,
	) -> Result<Columns> {
		let left_count = left.row_count();
		if left_count == 0 {
			return Ok(Columns::empty());
		}

		let right_row_number = right.row_numbers()[right_idx];

		let composite_keys: Vec<EncodedKey> = (0..left_count)
			.map(|left_idx| {
				let left_row_number = left.row_numbers()[left_idx];
				Self::make_composite_key(left_row_number, right_row_number)
			})
			.collect();

		let row_numbers_with_flags =
			txn.get_or_create_row_numbers(self.node, GroupId::NODE_SCOPE, &composite_keys)?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		Ok(builder.join_many_to_one(&row_numbers, left, right, right_idx))
	}

	pub(crate) fn join_columns_cartesian(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_indices: &[usize],
		right: &Columns,
		right_indices: &[usize],
	) -> Result<Columns> {
		let left_count = left_indices.len();
		let right_count = right_indices.len();
		if left_count == 0 || right_count == 0 {
			return Ok(Columns::empty());
		}

		let total_results = left_count * right_count;
		let mut composite_keys = Vec::with_capacity(total_results);

		for &left_idx in left_indices {
			let left_row_number = left.row_numbers()[left_idx];
			for &right_idx in right_indices {
				let right_row_number = right.row_numbers()[right_idx];
				composite_keys.push(Self::make_composite_key(left_row_number, right_row_number));
			}
		}

		let row_numbers_with_flags =
			txn.get_or_create_row_numbers(self.node, GroupId::NODE_SCOPE, &composite_keys)?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		Ok(builder.join_cartesian(&row_numbers, left, left_indices, right, right_indices))
	}

	pub(crate) fn join_left_with_slot(&self, left: &Columns, left_indices: &[usize], slot: &Columns) -> Columns {
		let row_numbers: Vec<RowNumber> = left_indices.iter().map(|&idx| left.row_numbers()[idx]).collect();
		let builder = JoinedColumnsBuilder::new(left, slot, &self.alias, self.natural);
		builder.join_cartesian(&row_numbers, left, left_indices, slot, &[0])
	}

	pub(crate) fn unmatched_left_latest(&self, left: &Columns, left_indices: &[usize]) -> Columns {
		let row_numbers: Vec<RowNumber> = left_indices.iter().map(|&idx| left.row_numbers()[idx]).collect();
		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		builder.unmatched_left_batch(&row_numbers, left, left_indices, &self.right_schema)
	}

	fn determine_side_from_origin(&self, origin: &ChangeOrigin) -> Option<JoinSide> {
		match origin {
			ChangeOrigin::Flow(from_node) => {
				if *from_node == self.left_node {
					Some(JoinSide::Left)
				} else if *from_node == self.right_node {
					Some(JoinSide::Right)
				} else {
					None
				}
			}
			_ => None,
		}
	}
}

impl RawStatefulOperator for JoinOperator {}

impl SingleStateful for JoinOperator {
	fn layout(&self) -> RowShape {
		self.shape.clone()
	}
}

impl Operator for JoinOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		Some(OperatorSample::default()
			.with_membership(self.membership.memory())
			.with_completeness(self.membership.completeness()))
	}

	fn invalidate_groups(&self, groups: &GroupSet) {
		if groups.is_empty() {
			return;
		}
		self.membership.invalidate();
	}

	fn retention_scale(&self) -> Option<Duration> {
		match (self.left_ttl, self.right_ttl) {
			(Some(left), Some(right)) => Some(left.max(right)),
			(Some(only), None) | (None, Some(only)) => Some(only),
			(None, None) => None,
		}
	}

	fn reclaimable_through(&self, _txn: &mut FlowTransaction, watermark: DateTime) -> Result<Reclaimable> {
		let behind = |span: Duration| watermark.saturating_sub(span);

		let mut keyspaces = Vec::new();
		if let Some(ttl) = self.left_ttl {
			keyspaces.push((Keyspace::JOIN_LEFT, behind(ttl)));
			keyspaces.extend(snapshot_ledger_keyspaces(self.snapshot)
				.into_iter()
				.map(|ks| (ks, behind(ttl))));
		}
		if let Some(ttl) = self.right_ttl
			&& !self.latest
		{
			keyspaces.push((Keyspace::JOIN_RIGHT, behind(ttl)));
		}

		Ok(Reclaimable {
			data: match (self.left_ttl, self.right_ttl) {
				(Some(left), Some(right)) => Some(behind(left.max(right))),
				_ => None,
			},
			keyspaces,
			mapping: match self.latest {
				true => None,
				false => self.left_ttl.map(behind),
			},
		})
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		if let ChangeOrigin::Flow(from_node) = &change.origin
			&& *from_node == self.node
		{
			return Ok(Change::from_flow(self.node, change.version, Vec::new(), DateTime::default()));
		}

		if self.natural && self.compiled_left_exprs.is_empty() {
			return Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at));
		}

		let mut state = JoinState::new(self.node, self.membership.clone(), self.snapshot);
		let mut result = Vec::with_capacity(change.diffs.len() * 2);

		let version = change.version;
		let parent_origin = change.origin.clone();
		for diff in change.diffs {
			let diff_origin = diff.origin().cloned().unwrap_or_else(|| parent_origin.clone());
			let side = self.determine_side_from_origin(&diff_origin).ok_or_else(|| {
				Error::from(FlowGraphError::UnknownDiffOrigin {
					operator: "Join",
					origin: None,
				})
			})?;
			let compiled_exprs = match side {
				JoinSide::Left => &self.compiled_left_exprs,
				JoinSide::Right => &self.compiled_right_exprs,
			};
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_join_insert(txn, &post, compiled_exprs, side, &mut state, &mut result)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_join_remove(txn, &pre, compiled_exprs, side, &mut state, &mut result)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_join_update(
					txn,
					&pre,
					&post,
					compiled_exprs,
					side,
					&mut state,
					&mut result,
				)?,
			}
		}

		Ok(Change::from_flow(self.node, version, result, change.changed_at))
	}
}

impl JoinOperator {
	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_join_insert(
		&self,
		txn: &mut FlowTransaction,
		post: &Columns,
		compiled_exprs: &[CompiledExpr],
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(post, compiled_exprs)?;

		if !self.latest {
			for (row_idx, key) in keys.iter().enumerate() {
				let mut ctx = JoinContext {
					side,
					state,
					operator: self,
				};
				let diffs = match key {
					Some(key_hash) => self.strategy.handle_insert(
						txn,
						post,
						&[row_idx],
						key_hash,
						&mut ctx,
					)?,
					None => self.strategy.handle_insert_undefined(txn, post, row_idx, &mut ctx)?,
				};
				result.extend(diffs);
			}
			return Ok(());
		}

		let (order, groups, undefined) = group_by_key(&keys);

		for key_hash in &order {
			let indices = &groups[key_hash];
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_insert(txn, post, indices, key_hash, &mut ctx)?);
		}

		for row_idx in undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_insert_undefined(txn, post, row_idx, &mut ctx)?);
		}

		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_join_remove(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		compiled_exprs: &[CompiledExpr],
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(pre, compiled_exprs)?;

		if !self.latest {
			for (row_idx, key) in keys.iter().enumerate() {
				let mut ctx = JoinContext {
					side,
					state,
					operator: self,
				};
				let diffs = match key {
					Some(key_hash) => {
						self.strategy.handle_remove(txn, pre, &[row_idx], key_hash, &mut ctx)?
					}
					None => self.strategy.handle_remove_undefined(txn, pre, row_idx, &mut ctx)?,
				};
				result.extend(diffs);
			}
			return Ok(());
		}

		let (order, groups, undefined) = group_by_key(&keys);

		for key_hash in &order {
			let indices = &groups[key_hash];
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_remove(txn, pre, indices, key_hash, &mut ctx)?);
		}

		for row_idx in undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_remove_undefined(txn, pre, row_idx, &mut ctx)?);
		}

		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_join_update(
		&self,
		txn: &mut FlowTransaction,
		pre: &Columns,
		post: &Columns,
		compiled_exprs: &[CompiledExpr],
		side: JoinSide,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let pre_keys = self.compute_join_keys(pre, compiled_exprs)?;
		let post_keys = self.compute_join_keys(post, compiled_exprs)?;
		let row_count = post.row_count();

		for row_idx in 0..row_count {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			let diffs = match (pre_keys[row_idx], post_keys[row_idx]) {
				(Some(pre_key), Some(post_key)) => {
					let keys = UpdateKeys {
						pre: &pre_key,
						post: &post_key,
					};
					self.strategy.handle_update(txn, pre, post, &[row_idx], keys, &mut ctx)?
				}
				(Some(pre_key), None) => {
					let mut diffs = self.strategy.handle_remove(
						txn,
						pre,
						&[row_idx],
						&pre_key,
						&mut ctx,
					)?;
					diffs.extend(self
						.strategy
						.handle_insert_undefined(txn, post, row_idx, &mut ctx)?);
					diffs
				}
				(None, Some(post_key)) => {
					let mut diffs =
						self.strategy.handle_remove_undefined(txn, pre, row_idx, &mut ctx)?;
					diffs.extend(self.strategy.handle_insert(
						txn,
						post,
						&[row_idx],
						&post_key,
						&mut ctx,
					)?);
					diffs
				}
				(None, None) => {
					self.strategy.handle_update_both_undefined(txn, pre, post, row_idx, &mut ctx)?
				}
			};
			result.extend(diffs);
		}

		Ok(())
	}
}

#[cfg(test)]
mod span_tests {
	use reifydb_codec::encoded::row::EncodedRow;
	use reifydb_core::{common::CommitVersion, state::horizon::Cutoff};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_flow::transaction::ChangeCoordinate;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::value::blob::Blob;

	use super::*;
	use crate::operator::join::store::{RowPresence, Store, group_bytes};

	// The spans below are read off the frontier the operator reports, so every expectation is an
	// instant behind a fixed watermark rather than a bare duration. Anchoring the watermark here
	// keeps `WATERMARK - ttl` readable at each call site.
	fn watermark() -> DateTime {
		DateTime::from_millis(10_000_000)
	}

	fn behind(span: Duration) -> DateTime {
		watermark().saturating_sub(span)
	}

	fn frontier(op: &JoinOperator, engine: &TestEngine) -> Reclaimable {
		let mut txn = engine.flow_txn().deferred();
		op.reclaimable_through(&mut txn, watermark()).expect("the join reports a frontier")
	}

	#[test]
	fn each_declared_side_ttl_becomes_a_span_on_that_sides_keyspace_when_not_snapshotting() {
		// This is the whole contract the join hands the reclaim sweep: which keyspace ages, and how
		// far behind the flow watermark it retires. Naming the wrong keyspace here silently reclaims
		// the other side's rows, and a side declared but omitted never ages at all.
		//
		// Snapshot is off so this stays about the two sides; the ledger keyspaces a snapshot join
		// adds on top have their own test.
		let engine = TestEngine::new();
		let left = Duration::from_seconds(60).unwrap();
		let right = Duration::from_seconds(3_600).unwrap();

		let mut op = make_op(70, Some(left), Some(right), &engine);
		op.snapshot = false;

		assert_eq!(
			frontier(&op, &engine).keyspaces,
			vec![(Keyspace::JOIN_LEFT, behind(left)), (Keyspace::JOIN_RIGHT, behind(right))],
			"each side must age on its own keyspace at its own declared ttl"
		);
	}

	#[test]
	fn a_side_without_a_ttl_declares_no_span_when_not_snapshotting() {
		// No ttl means the side is bounded by the node's own horizon, not by a per-side sweep.
		// Declaring a span here would retire rows the user never asked to expire.
		let engine = TestEngine::new();
		let left = Duration::from_seconds(60).unwrap();

		let mut untimed_right = make_op(71, Some(left), None, &engine);
		untimed_right.snapshot = false;
		assert_eq!(
			frontier(&untimed_right, &engine).keyspaces,
			vec![(Keyspace::JOIN_LEFT, behind(left))],
			"the untimed right side must not be enrolled"
		);

		let mut untimed = make_op(72, None, None, &engine);
		untimed.snapshot = false;
		assert!(
			frontier(&untimed, &engine).keyspaces.is_empty(),
			"a join with no ttl at all ages only on the node horizon"
		);
	}

	#[test]
	fn a_join_naming_only_one_side_still_reports_a_scale_but_no_data_frontier() {
		// Two properties that must not be conflated. The GROUP data range cannot be erased while
		// the undeclared side is still probeable, so there is no data frontier - erasing it would
		// drop rows the other side can still match. But the declared side does age on its own
		// keyspace, and that sweep buckets activity on a grid sized from the node's scale. Reporting
		// no scale would stamp every side entry into one bucket, so the sweep could neither locate
		// them nor bound them - which is how a left-only ttl silently never evicted and a stale left
		// row kept rejoining fresh right rows.
		let engine = TestEngine::new();
		let left = ttl(1_000);

		let mut op = make_op(90, Some(left), None, &engine);
		op.snapshot = false;

		assert_eq!(op.retention_scale(), Some(left), "the side sweep needs an event-time grid to stamp on");
		assert_eq!(frontier(&op, &engine).data, None, "one declared side cannot retire the shared group");
	}

	#[test]
	fn a_frontier_saturates_while_the_watermark_is_younger_than_the_ttl() {
		// Early in a node's life the watermark is below the declared ttl. Wrapping would put the
		// frontier near u64::MAX and make every group due on the first tick, wiping state before a
		// single row aged out. Every operator subtracts its span from the watermark this way, so the
		// saturation has to hold at the subtraction, not at the sweep.
		let engine = TestEngine::new();
		let mut op = make_op(94, Some(ttl(60_000)), Some(ttl(60_000)), &engine);
		op.snapshot = false;

		let mut txn = engine.flow_txn().deferred();
		let young = op.reclaimable_through(&mut txn, DateTime::from_millis(1_000)).unwrap();

		assert_eq!(young.data, Some(DateTime::EPOCH), "a watermark below the ttl floors at the epoch");
		assert_eq!(
			young.keyspaces,
			vec![(Keyspace::JOIN_LEFT, DateTime::EPOCH), (Keyspace::JOIN_RIGHT, DateTime::EPOCH),]
		);
	}

	#[test]
	fn the_scale_takes_the_longer_side_so_slack_is_never_understated() {
		// Slack is one bucket width and a width is scale/16, so a grid derived from the SHORTER side
		// would understate the longer side's slack and retire a group mid-bucket, while a coarser
		// grid can only ever delay. The scale therefore takes the max, never the min and never an
		// average, whichever order the sides are declared in.
		let engine = TestEngine::new();

		assert_eq!(
			make_op(91, Some(ttl(600_000)), Some(ttl(1_000)), &engine).retention_scale(),
			Some(ttl(600_000))
		);
		assert_eq!(
			make_op(92, Some(ttl(1_000)), Some(ttl(600_000)), &engine).retention_scale(),
			Some(ttl(600_000))
		);
		assert_eq!(
			make_op(93, None, None, &engine).retention_scale(),
			None,
			"a node that declares nothing anywhere stays ungridded and is skipped entirely"
		);
	}

	#[test]
	fn the_node_scope_mapping_ages_on_the_left_ttl_and_not_at_all_without_one() {
		// The mapping is minted per (left,right) output pair and keyed by the left row, so the left
		// ttl is the only span that bounds it - this is the declaration that replaces the join's own
		// evict_rownumbers. Declaring the right ttl here would drop mappings whose left row is still
		// live and the join would re-mint a second row number for a row that already exists;
		// declaring nothing reinstates the unbounded growth the sweep exists to stop.
		let engine = TestEngine::new();
		let left = ttl(50);

		assert_eq!(
			frontier(&make_op(74, Some(left), Some(ttl(9_000)), &engine), &engine).mapping,
			Some(behind(left))
		);
		assert_eq!(
			frontier(&make_op(75, None, Some(ttl(9_000)), &engine), &engine).mapping,
			None,
			"a join with no left ttl declares no mapping span"
		);
	}

	#[test]
	fn a_latest_join_never_ages_its_mapping() {
		// Latest reuses the left row's number for the emitted row rather than minting a composite,
		// so there is no per-pair mapping to age; sweeping here would evict the identity of rows the
		// join is still emitting under.
		let engine = TestEngine::new();
		let mut op = make_op(76, Some(ttl(50)), None, &engine);
		op.latest = true;

		assert_eq!(frontier(&op, &engine).mapping, None);
	}

	#[test]
	fn a_latest_join_never_ages_its_right_side_when_not_snapshotting() {
		// In latest mode the right side is a one-row-per-key slot the join reads on every left
		// arrival, so it is state the operator depends on rather than a window of history. Ageing it
		// would make a left row silently stop matching a right row that is still current - the same
		// carve-out the eviction path it replaces made for `latest`.
		let engine = TestEngine::new();
		let mut op = make_op(
			73,
			Some(Duration::from_seconds(60).unwrap()),
			Some(Duration::from_seconds(60).unwrap()),
			&engine,
		);
		op.latest = true;
		op.snapshot = false;

		assert_eq!(
			frontier(&op, &engine).keyspaces,
			vec![(Keyspace::JOIN_LEFT, behind(Duration::from_seconds(60).unwrap()))],
			"latest mode declares the left span and withholds the right"
		);
	}

	#[test]
	fn a_snapshot_join_ages_its_ledger_on_the_left_span_and_declares_it_after_the_left_side() {
		// The ledger records what each left row published, so it is only meaningful for as long as
		// the left row it describes - hence the left ttl, not a span of its own.
		//
		// The order matters just as much. The sweep walks these under one shared budget and stops
		// where the budget runs out, so JOIN_LEFT going first means a cut-off sweep leaves published
		// records whose left rows are already gone, and the next sweep clears them. Declared the
		// other way round, a cut-off sweep strips live left rows of the record of what they
		// published, and the joined rows they own can never be withdrawn again.
		let engine = TestEngine::new();
		let left = ttl(50);

		let op = make_op(77, Some(left), Some(ttl(9_000)), &engine);

		assert_eq!(
			frontier(&op, &engine).keyspaces,
			vec![
				(Keyspace::JOIN_LEFT, behind(left)),
				(Keyspace::JOIN_PUBLISHED, behind(left)),
				(Keyspace::JOIN_PIN, behind(left)),
				(Keyspace::JOIN_RIGHT, behind(ttl(9_000))),
			],
			"the ledger must age with the left side and be swept after it"
		);
	}

	#[test]
	fn storing_a_left_row_stamps_the_snapshot_ledger_even_when_it_publishes_nothing() {
		// Equal spans only bound the ledger if the two clocks also start together. Publishing is the
		// obvious place to stamp the ledger, but a left row whose key matches no right row publishes
		// nothing at all, while its own keyspace is stamped regardless. That gap is enough to break
		// it: a group whose last publish is older than its last left write has its ledger fall due
		// first, and the sweep deletes the published records of left rows that are still live. Those
		// rows then withdraw nothing when they are removed and their joined rows are stranded in the
		// view forever - the exact defect the ledger exists to prevent.
		//
		// So the stamp rides the left write, not the publish. No right row is stored here at all.
		let engine = TestEngine::new();
		let op = make_op(78, Some(ttl(50)), None, &engine);
		let mut txn = engine.flow_txn().deferred();

		let left = Store::new(op.node, JoinSide::Left, op.membership.clone())
			.also_stamping(snapshot_ledger_keyspaces(true));
		let hash = Hash128(0xFEED);
		at(&mut txn, 10);
		left.put_row(&mut txn, &hash, RowNumber(1), &op_row(0x01), RowPresence::Unknown).unwrap();

		let group = txn
			.lookup_group(op.node, &group_bytes(&hash))
			.unwrap()
			.expect("a stored left row must have interned its key");
		let cutoff = Cutoff(DateTime::from_millis(20));
		for keyspace in [Keyspace::JOIN_LEFT, Keyspace::JOIN_PUBLISHED, Keyspace::JOIN_PIN] {
			assert_eq!(
				txn.due_side_groups(op.node, keyspace, cutoff, 16).unwrap(),
				vec![group],
				"{keyspace:?} must fall due with the left side it describes, not on its own clock"
			);
		}
	}

	#[test]
	fn a_plain_join_leaves_the_snapshot_ledger_keyspaces_alone() {
		// Without snapshot nothing is ever written to the ledger, so stamping it would enrol a group
		// in a sweep with no records to reclaim and no left row depending on it.
		let engine = TestEngine::new();
		let op = make_op(79, Some(ttl(50)), None, &engine);
		let mut txn = engine.flow_txn().deferred();

		let left = Store::new(op.node, JoinSide::Left, op.membership.clone())
			.also_stamping(snapshot_ledger_keyspaces(false));
		let hash = Hash128(0xBEEF);
		at(&mut txn, 10);
		left.put_row(&mut txn, &hash, RowNumber(1), &op_row(0x01), RowPresence::Unknown).unwrap();

		let cutoff = Cutoff(DateTime::from_millis(20));
		assert!(!txn.due_side_groups(op.node, Keyspace::JOIN_LEFT, cutoff, 16).unwrap().is_empty());
		for keyspace in [Keyspace::JOIN_PUBLISHED, Keyspace::JOIN_PIN] {
			assert!(
				txn.due_side_groups(op.node, keyspace, cutoff, 16).unwrap().is_empty(),
				"{keyspace:?} must stay unenrolled on a join that never publishes to it"
			);
		}
	}

	fn ttl(millis: i64) -> Duration {
		Duration::from_milliseconds_const(millis)
	}

	// Mappings are stamped from the transaction's change coordinate, not the clock, so these tests
	// place a write in event time by setting it rather than by advancing a mock clock.
	fn at(txn: &mut FlowTransaction, millis: u64) {
		txn.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(millis),
			version: CommitVersion(0),
		});
	}

	fn make_op(
		node: u64,
		left_ttl: Option<Duration>,
		right_ttl: Option<Duration>,
		engine: &TestEngine,
	) -> JoinOperator {
		// No version epoch is seeded any more: nothing the join ages resolves a cutoff through it.
		// Spans are declared against the flow watermark and the sweep applies them, so these tests
		// place writes in event time via `at` rather than by seeding an epoch and advancing a clock.
		let routines = engine.executor().routines.clone();
		let rc = RuntimeContext::with_clock(engine.clock().clone());
		JoinOperator::new_for_state_tests(FlowNodeId(node), left_ttl, right_ttl, routines, rc)
	}

	fn op_row(payload: u8) -> EncodedRow {
		let shape = RowShape::operator_state();
		let mut r = shape.allocate();
		shape.set_blob(&mut r, 0, &Blob::from(vec![payload]));
		r
	}

	#[test]
	fn the_mapping_sweep_evicts_rownumbers_past_the_left_ttl() {
		// A join mints one row-number mapping per (left,right) output pair. If those mappings are
		// never evicted once the left row ages past the left TTL, the join's internal state grows
		// without bound (observed: 430M mapping rows / 66GB on a live ingestor). The sweep now runs
		// off the flow watermark rather than a tick, so the mapping's own #time decides - which is
		// what makes the bound hold during a replay too, where the version-anchored sweep aged
		// mappings by how recently they were INGESTED and so never fired.
		let engine = TestEngine::new();
		let op = make_op(30, Some(ttl(50)), None, &engine);
		let mut txn = engine.flow_txn().deferred();

		let old = JoinOperator::make_composite_key(RowNumber(1), RowNumber(1));
		at(&mut txn, 0);
		txn.get_or_create_row_number(op.node, GroupId::NODE_SCOPE, &old).unwrap();

		let young = JoinOperator::make_composite_key(RowNumber(2), RowNumber(1));
		at(&mut txn, 40);
		txn.get_or_create_row_number(op.node, GroupId::NODE_SCOPE, &young).unwrap();

		let mut cursor = None;
		txn.evict_row_numbers(
			op.node,
			GroupId::NODE_SCOPE,
			Cutoff(DateTime::from_millis(10)),
			&mut cursor,
			100,
		)
		.unwrap();

		assert!(
			txn.get_row_number(op.node, GroupId::NODE_SCOPE, &old).unwrap().is_none(),
			"a mapping stamped at or before the cutoff must be evicted"
		);
		assert!(
			txn.get_row_number(op.node, GroupId::NODE_SCOPE, &young).unwrap().is_some(),
			"a mapping stamped after the cutoff must survive; the version-anchored sweep could not \
			 express this and evicted both"
		);
	}

	#[test]
	fn group_reclamation_drops_every_instance_the_substrate_deleted() {
		// The substrate deletes a reclaimed group's rows itself and hands the operator only
		// the group id - no transaction, and no count of how many rows went. remove()
		// decrements a single instance, so a key that held two rows would strand one of
		// them and read maybe-present on every probe for the rest of the run, decaying the
		// side into the permanent read-through the filter exists to prevent. Invalidating
		// and re-scanning is the only correction available at that callback.
		let engine = TestEngine::new();
		let op = make_op(60, None, None, &engine);
		let mut txn = engine.flow_txn().deferred();

		let membership = op.membership.clone();
		let left = Store::new(op.node, JoinSide::Left, membership.clone());
		let hash = Hash128(0xABC);
		left.put_row(&mut txn, &hash, RowNumber(1), &op_row(0x10), RowPresence::Unknown).unwrap();
		left.put_row(&mut txn, &hash, RowNumber(2), &op_row(0x20), RowPresence::Unknown).unwrap();

		let group = txn
			.lookup_group(op.node, &group_bytes(&hash))
			.unwrap()
			.expect("a stored key must have been interned");
		txn.reclaim_group_data(op.node, group, 128).unwrap();
		op.invalidate_groups(&GroupSet::new([group]));

		let absences_before = membership.side(JoinSide::Left).completeness().absences_served.as_u64();
		assert!(!left.contains_key(&mut txn, &hash).unwrap());
		assert_eq!(
			membership.side(JoinSide::Left).completeness().absences_served.as_u64(),
			absences_before + 1,
			"a reclaimed key must come back as a RAM absence, not a lingering maybe-present"
		);
	}

	#[test]
	fn the_mapping_sweep_preserves_the_row_number_counter() {
		// Evicting every mapping must NOT reset the monotonic counter; a fresh mapping after a
		// full eviction must get a strictly larger number, or a recycled id would corrupt any
		// downstream consumer that tracks rows by number. The counter lives in its own node-scope
		// keyspace precisely so a mapping sweep cannot reach it.
		let engine = TestEngine::new();
		let op = make_op(30, Some(ttl(50)), None, &engine);
		let mut txn = engine.flow_txn().deferred();

		let first = JoinOperator::make_composite_key(RowNumber(1), RowNumber(1));
		at(&mut txn, 0);
		let (n1, _) = txn.get_or_create_row_number(op.node, GroupId::NODE_SCOPE, &first).unwrap();

		let mut cursor = None;
		txn.evict_row_numbers(
			op.node,
			GroupId::NODE_SCOPE,
			Cutoff(DateTime::from_millis(100)),
			&mut cursor,
			100,
		)
		.unwrap();
		assert!(txn.get_row_number(op.node, GroupId::NODE_SCOPE, &first).unwrap().is_none());

		let second = JoinOperator::make_composite_key(RowNumber(7), RowNumber(7));
		at(&mut txn, 200);
		let (n2, is_new) = txn.get_or_create_row_number(op.node, GroupId::NODE_SCOPE, &second).unwrap();
		assert!(is_new);
		assert!(n2.0 > n1.0, "counter must keep advancing past evicted mappings, not recycle ids");
	}
}
