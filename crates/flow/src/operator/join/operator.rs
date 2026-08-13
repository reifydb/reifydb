// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use postcard::to_extend;
use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_core::{
	common::JoinType,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	key::operator_state::GroupId,
	metrics::heap::OperatorSample,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::{Hash128, xxh3_128},
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber, value_type::ValueType},
};
use tracing::instrument;

use super::{
	column::JoinedColumnsBuilder,
	snapshot::SnapshotLedger,
	state::{JoinSide, JoinState},
	strategy::{JoinContext, JoinStrategy, UpdateKeys},
};
use crate::{
	context::FlowContext,
	error::{FlowGraphError, FlowStateError},
	operator::{
		HostOperator,
		host::HostContext,
		join::{Emitted, Identity},
	},
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

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
		// Latest reuses the left row's number, so a first-occurrence order is what keeps output
		// identity stable; every input index must land in exactly one group.
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
	pub operator: OperatorId,
	pub exprs: Vec<Expression>,
	pub schema: Columns,
}

pub struct JoinOperator {
	operator: OperatorId,
	strategy: JoinStrategy,
	left_node: OperatorId,
	right_node: OperatorId,
	compiled_left_exprs: Vec<CompiledExpr>,
	compiled_right_exprs: Vec<CompiledExpr>,
	alias: Option<String>,
	right_schema: Columns,
	routines: Routines,
	runtime_context: RuntimeContext,
	pub(crate) snapshot: bool,
	natural: bool,
	pub(crate) latest: bool,
	_left_seal: Option<Duration>,
	_right_seal: Option<Duration>,
	ctx: Arc<FlowContext>,
}

impl JoinOperator {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		left: JoinSideConfig,
		right: JoinSideConfig,
		operator: OperatorId,
		join_type: JoinType,
		alias: Option<String>,
		routines: Routines,
		runtime_context: RuntimeContext,
		snapshot: bool,
		natural: bool,
		latest: bool,
		left_seal: Option<Duration>,
		right_seal: Option<Duration>,
		ctx: Arc<FlowContext>,
	) -> Self {
		let left_node = left.operator;
		let right_node = right.operator;
		let left_exprs = left.exprs;
		let right_exprs = right.exprs;
		let right_schema = right.schema;
		let strategy = JoinStrategy::from(join_type, latest);

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

		Self {
			operator,
			strategy,
			left_node,
			right_node,
			compiled_left_exprs,
			compiled_right_exprs,
			alias,
			right_schema,
			routines,
			runtime_context,
			snapshot,
			natural,
			latest,
			_left_seal: left_seal,
			_right_seal: right_seal,
			ctx,
		}
	}

	pub(crate) fn snapshot_ledger(&self) -> SnapshotLedger {
		SnapshotLedger::new()
	}

	#[instrument(name = "flow::operator::join::compute_keys", level = "trace", skip_all, fields(rows = columns.row_count()))]
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
		host: &mut dyn HostContext,
		left: &Columns,
		left_idx: usize,
		identity: Identity,
	) -> Result<Emitted> {
		let left_row_number = left.row_numbers()[left_idx];

		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_row_number.0);
		let composite_key = serializer.finish();

		let (row_numbers, fresh, existing) = self.identities(host, &[composite_key], identity)?;
		if fresh.is_empty() && existing.is_empty() {
			return Ok(Emitted::empty());
		}

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		let built = builder.unmatched_left(row_numbers[0], left, left_idx, &self.right_schema);
		Ok(Self::split(built, &fresh, &existing))
	}

	fn identities(
		&self,
		host: &mut dyn HostContext,
		keys: &[EncodedKey],
		identity: Identity,
	) -> Result<(Vec<RowNumber>, Vec<usize>, Vec<usize>)> {
		match identity {
			Identity::Mint => {
				let minted = host.get_or_create_row_numbers(GroupId::ROOT, keys)?;
				let (fresh, existing) = (0..keys.len()).partition(|index| minted[*index].1);
				Ok((minted.iter().map(|(number, _)| *number).collect(), fresh, existing))
			}
			Identity::Existing | Identity::Consume => {
				let resolved = host.get_row_numbers(GroupId::ROOT, keys)?;
				let existing: Vec<usize> = resolved
					.iter()
					.enumerate()
					.filter_map(|(index, number)| number.map(|_| index))
					.collect();
				if identity == Identity::Consume {
					for index in &existing {
						host.remove_row_number(GroupId::ROOT, &keys[*index])?;
					}
				}
				Ok((
					resolved.into_iter().map(|number| number.unwrap_or(RowNumber(0))).collect(),
					Vec::new(),
					existing,
				))
			}
		}
	}

	fn split(built: Columns, fresh: &[usize], existing: &[usize]) -> Emitted {
		Emitted {
			fresh: JoinedColumnsBuilder::retain_rows(&built, fresh),
			existing: JoinedColumnsBuilder::retain_rows(&built, existing),
		}
	}

	pub(crate) fn unmatched_left_columns_batch(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		left_indices: &[usize],
		identity: Identity,
	) -> Result<Emitted> {
		if left_indices.is_empty() {
			return Ok(Emitted::empty());
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

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		let built = builder.unmatched_left_batch(&row_numbers, left, left_indices, &self.right_schema);
		Ok(Self::split(built, &fresh, &existing))
	}

	pub(crate) fn cleanup_left_row_joins(&self, host: &mut dyn HostContext, left_number: u64) -> Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		host.remove_row_numbers_by_prefix(GroupId::ROOT, &prefix)
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
		host: &mut dyn HostContext,
		left: &Columns,
		left_idx: usize,
		right: &Columns,
		identity: Identity,
	) -> Result<Emitted> {
		let right_count = right.row_count();
		if right_count == 0 {
			return Ok(Emitted::empty());
		}

		let left_row_number = left.row_numbers()[left_idx];

		let composite_keys: Vec<EncodedKey> = (0..right_count)
			.map(|right_idx| {
				let right_row_number = right.row_numbers()[right_idx];
				Self::make_composite_key(left_row_number, right_row_number)
			})
			.collect();

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		let built = builder.join_one_to_many(&row_numbers, left, left_idx, right);
		Ok(Self::split(built, &fresh, &existing))
	}

	pub(crate) fn join_columns_many_to_one(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		right: &Columns,
		right_idx: usize,
		identity: Identity,
	) -> Result<Emitted> {
		let left_count = left.row_count();
		if left_count == 0 {
			return Ok(Emitted::empty());
		}

		let right_row_number = right.row_numbers()[right_idx];

		let composite_keys: Vec<EncodedKey> = (0..left_count)
			.map(|left_idx| {
				let left_row_number = left.row_numbers()[left_idx];
				Self::make_composite_key(left_row_number, right_row_number)
			})
			.collect();

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		let built = builder.join_many_to_one(&row_numbers, left, right, right_idx);
		Ok(Self::split(built, &fresh, &existing))
	}

	pub(crate) fn join_columns_cartesian(
		&self,
		host: &mut dyn HostContext,
		left: &Columns,
		left_indices: &[usize],
		right: &Columns,
		right_indices: &[usize],
		identity: Identity,
	) -> Result<Emitted> {
		let left_count = left_indices.len();
		let right_count = right_indices.len();
		if left_count == 0 || right_count == 0 {
			return Ok(Emitted::empty());
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

		let (row_numbers, fresh, existing) = self.identities(host, &composite_keys, identity)?;

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias, self.natural);
		let built = builder.join_cartesian(&row_numbers, left, left_indices, right, right_indices);
		Ok(Self::split(built, &fresh, &existing))
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

impl HostOperator for JoinOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		Some(OperatorSample::default())
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		if let ChangeOrigin::Flow(from_node) = &change.origin
			&& *from_node == self.operator
		{
			return Ok(Change::from_flow(self.operator, change.version, Vec::new(), DateTime::default()));
		}

		if self.natural && self.compiled_left_exprs.is_empty() {
			return Ok(Change::from_flow(self.operator, change.version, Vec::new(), change.changed_at));
		}

		let mut state = JoinState::new();
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
				} => self.apply_join_insert(host, &post, compiled_exprs, side, &mut state, &mut result)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_join_remove(host, &pre, compiled_exprs, side, &mut state, &mut result)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_join_update(
					host,
					&pre,
					&post,
					compiled_exprs,
					side,
					&mut state,
					&mut result,
				)?,
			}
		}

		Ok(Change::from_flow(self.operator, version, result, change.changed_at))
	}
}

impl JoinOperator {
	#[inline]
	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "flow::operator::join::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_join_insert(
		&self,
		host: &mut dyn HostContext,
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
						host,
						post,
						&[row_idx],
						key_hash,
						&mut ctx,
					)?,
					None => self.strategy.handle_insert_undefined(host, post, row_idx, &mut ctx)?,
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
			result.extend(self.strategy.handle_insert(host, post, indices, key_hash, &mut ctx)?);
		}

		for row_idx in undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_insert_undefined(host, post, row_idx, &mut ctx)?);
		}

		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "flow::operator::join::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn apply_join_remove(
		&self,
		host: &mut dyn HostContext,
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
					Some(key_hash) => self.strategy.handle_remove(
						host,
						pre,
						&[row_idx],
						key_hash,
						&mut ctx,
					)?,
					None => self.strategy.handle_remove_undefined(host, pre, row_idx, &mut ctx)?,
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
			result.extend(self.strategy.handle_remove(host, pre, indices, key_hash, &mut ctx)?);
		}

		for row_idx in undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			result.extend(self.strategy.handle_remove_undefined(host, pre, row_idx, &mut ctx)?);
		}

		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	#[instrument(name = "flow::operator::join::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_join_update(
		&self,
		host: &mut dyn HostContext,
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
					self.strategy.handle_update(host, pre, post, &[row_idx], keys, &mut ctx)?
				}
				(Some(pre_key), None) => {
					let mut diffs = self.strategy.handle_remove(
						host,
						pre,
						&[row_idx],
						&pre_key,
						&mut ctx,
					)?;
					diffs.extend(self
						.strategy
						.handle_insert_undefined(host, post, row_idx, &mut ctx)?);
					diffs
				}
				(None, Some(post_key)) => {
					let mut diffs =
						self.strategy.handle_remove_undefined(host, pre, row_idx, &mut ctx)?;
					diffs.extend(self.strategy.handle_insert(
						host,
						post,
						&[row_idx],
						&post_key,
						&mut ctx,
					)?);
					diffs
				}
				(None, None) => self
					.strategy
					.handle_update_both_undefined(host, pre, post, row_idx, &mut ctx)?,
			};
			result.extend(diffs);
		}

		Ok(())
	}
}
