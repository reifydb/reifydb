// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, collections::HashMap, sync::LazyLock};

use postcard::to_extend;
use reifydb_core::interface::flow::OperatorCapability;
use reifydb_codec::{
	row::shape::{RowFamily, RowShape, RowShapeField},
	key::{encoded::EncodedKey, serializer::KeySerializer},
};
use reifydb_core::{
	common::{CommitVersion, JoinType},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_evaluate::expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	};
use reifydb_evaluate::stack::SymbolTable;
use crate::vm::executor::Executor;
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::version_epoch::EpochSeconds;
use reifydb_runtime::context::RuntimeContext;
use reifydb_sdk::flow::operator::timer::Timer as Tick;
use reifydb_value::{
	Result,
	error::Error,
	params::Params,
	util::hash::{Hash128, xxh3_128},
	value::{
		Value, datetime::DateTime, duration::Duration, identity::IdentityId, row_number::RowNumber,
		value_type::ValueType,
	},
};

use super::{
	column::JoinedColumnsBuilder,
	state::{JoinSide, JoinState},
	store::Store,
	strategy::{JoinContext, JoinStrategy, UpdateKeys},
};
use crate::flow::{
	error::{FlowGraphError, FlowStateError},
	operator::{
		Operator,
		stateful::{raw::RawStatefulOperator, row::RowNumberProvider, single::SingleStateful},
	},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

pub(crate) const EVICT_BATCH: usize = 4096;

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
	pub node: OperatorId,
	pub exprs: Vec<Expression>,
	pub schema: Columns,
}

pub struct JoinOperator {
	node: OperatorId,
	strategy: JoinStrategy,
	left_node: OperatorId,
	right_node: OperatorId,
	compiled_left_exprs: Vec<CompiledExpr>,
	compiled_right_exprs: Vec<CompiledExpr>,
	alias: Option<String>,
	shape: RowShape,
	right_schema: Columns,
	row_number_provider: RowNumberProvider,
	routines: Routines,
	runtime_context: RuntimeContext,
	pub(crate) snapshot: bool,
	natural: bool,
	pub(crate) latest: bool,
	left_ttl: Option<Duration>,
	right_ttl: Option<Duration>,
	left_evict_cursor: RefCell<Option<EncodedKey>>,
	right_evict_cursor: RefCell<Option<EncodedKey>>,
	rownumber_evict_cursor: RefCell<Option<EncodedKey>>,
}

impl JoinOperator {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		left: JoinSideConfig,
		right: JoinSideConfig,
		node: OperatorId,
		join_type: JoinType,
		alias: Option<String>,
		executor: Executor,
		snapshot: bool,
		natural: bool,
		latest: bool,
		left_ttl: Option<Duration>,
		right_ttl: Option<Duration>,
	) -> Self {
		let left_node = left.node;
		let right_node = right.node;
		let left_exprs = left.exprs;
		let right_exprs = right.exprs;
		let right_schema = right.schema;
		let strategy = JoinStrategy::from(join_type, latest);
		let shape = Self::state_shape();
		let row_number_provider = RowNumberProvider::new(node);

		let compile_ctx = CompileContext {
			symbols: &EMPTY_SYMBOL_TABLE,
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
			row_number_provider,
			routines,
			runtime_context,
			snapshot,
			natural,
			latest,
			left_ttl,
			right_ttl,
			left_evict_cursor: RefCell::new(None),
			right_evict_cursor: RefCell::new(None),
			rownumber_evict_cursor: RefCell::new(None),
		}
	}

	fn state_shape() -> RowShape {
		RowShape::new(RowFamily::Operator, vec![RowShapeField::unconstrained("state", ValueType::Blob)])
	}

	#[cfg(test)]
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new_for_state_tests(
		node: OperatorId,
		left_ttl: Option<Duration>,
		right_ttl: Option<Duration>,
		routines: Routines,
		runtime_context: RuntimeContext,
	) -> Self {
		Self {
			node,
			strategy: JoinStrategy::from(JoinType::Inner, false),
			left_node: OperatorId(0),
			right_node: OperatorId(0),
			compiled_left_exprs: Vec::new(),
			compiled_right_exprs: Vec::new(),
			alias: None,
			shape: Self::state_shape(),
			right_schema: Columns::empty(),
			row_number_provider: RowNumberProvider::new(node),
			routines,
			runtime_context,
			snapshot: true,
			natural: false,
			latest: false,
			left_ttl,
			right_ttl,
			left_evict_cursor: RefCell::new(None),
			right_evict_cursor: RefCell::new(None),
			rownumber_evict_cursor: RefCell::new(None),
		}
	}

	fn evict_left(&self, txn: &mut FlowTransaction, now: DateTime) -> Result<()> {
		let Some(ttl) = self.left_ttl else {
			return Ok(());
		};
		let Some(cutoff_nanos) = now.to_nanos().checked_sub(ttl.get_nanos() as u64) else {
			return Ok(());
		};
		let Some(cutoff_version) =
			self.runtime_context.version_epoch.floor_version_at(EpochSeconds::from_nanos(cutoff_nanos)).map(CommitVersion)
		else {
			return Ok(());
		};
		let left = Store::new(self.node, JoinSide::Left);
		let mut cursor = self.left_evict_cursor.borrow_mut().take();
		left.evict_expired(txn, cutoff_version, &mut cursor, EVICT_BATCH)?;
		*self.left_evict_cursor.borrow_mut() = cursor;
		Ok(())
	}

	fn evict_right(&self, txn: &mut FlowTransaction, now: DateTime) -> Result<()> {
		let Some(ttl) = self.right_ttl else {
			return Ok(());
		};
		let Some(cutoff_nanos) = now.to_nanos().checked_sub(ttl.get_nanos() as u64) else {
			return Ok(());
		};
		let Some(cutoff_version) =
			self.runtime_context.version_epoch.floor_version_at(EpochSeconds::from_nanos(cutoff_nanos)).map(CommitVersion)
		else {
			return Ok(());
		};
		let right = Store::new(self.node, JoinSide::Right);
		let mut cursor = self.right_evict_cursor.borrow_mut().take();
		right.evict_expired(txn, cutoff_version, &mut cursor, EVICT_BATCH)?;
		*self.right_evict_cursor.borrow_mut() = cursor;
		Ok(())
	}

	fn evict_rownumbers(&self, txn: &mut FlowTransaction, now: DateTime) -> Result<()> {
		let Some(ttl) = self.left_ttl else {
			return Ok(());
		};
		let Some(cutoff_nanos) = now.to_nanos().checked_sub(ttl.get_nanos() as u64) else {
			return Ok(());
		};
		let Some(cutoff_version) =
			self.runtime_context.version_epoch.floor_version_at(EpochSeconds::from_nanos(cutoff_nanos)).map(CommitVersion)
		else {
			return Ok(());
		};
		let mut cursor = self.rownumber_evict_cursor.borrow_mut().take();
		self.row_number_provider.evict_expired(txn, cutoff_version, &mut cursor, EVICT_BATCH)?;
		*self.rownumber_evict_cursor.borrow_mut() = cursor;
		Ok(())
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
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: IdentityId::root(),
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
			self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;

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
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias, self.natural);
		Ok(builder.unmatched_left_batch(&row_numbers, left, left_indices, &self.right_schema))
	}

	pub(crate) fn cleanup_left_row_joins(&self, txn: &mut FlowTransaction, left_number: u64) -> Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		self.row_number_provider.remove_by_prefix(txn, &prefix)
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
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
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
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
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
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
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
	fn id(&self) -> OperatorId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn ticks(&self) -> Option<Duration> {
		if self.left_ttl.is_some() || self.right_ttl.is_some() {
			Some(Duration::from_seconds(1).unwrap())
		} else {
			None
		}
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		self.evict_left(txn, tick.due)?;
		if !self.latest {
			self.evict_right(txn, tick.due)?;
			self.evict_rownumbers(txn, tick.due)?;
		}
		Ok(None)
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

		let mut state = JoinState::new(self.node);
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
				_ => self.strategy.handle_update_undefined(txn, pre, post, row_idx, &mut ctx)?,
			};
			result.extend(diffs);
		}

		Ok(())
	}
}
