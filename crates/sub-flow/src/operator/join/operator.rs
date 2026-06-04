// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, sync::LazyLock};

use postcard::to_stdvec;
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	common::JoinType,
	encoded::{key::EncodedKey, shape::RowShape},
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin, Diff},
	},
	internal,
	row::TtlAnchor,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::{executor::Executor, stack::SymbolTable},
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::{
	context::RuntimeContext,
	hash::{Hash128, xxh3_128},
};
use reifydb_sdk::operator::Tick;
use reifydb_value::{
	Result,
	error::Error,
	params::Params,
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
use crate::{
	operator::{
		Operator,
		stateful::{raw::RawStatefulOperator, row::RowNumberProvider, single::SingleStateful},
	},
	transaction::FlowTransaction,
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

pub(crate) const EVICT_BATCH: usize = 4096;

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
	row_number_provider: RowNumberProvider,
	routines: Routines,
	runtime_context: RuntimeContext,
	pub(crate) snapshot: bool,
	natural: bool,
	pub(crate) latest: bool,
	left_ttl: Option<Duration>,
	left_ttl_anchor: TtlAnchor,
	right_ttl: Option<Duration>,
	right_ttl_anchor: TtlAnchor,
	left_evict_cursor: RefCell<Option<EncodedKey>>,
	right_evict_cursor: RefCell<Option<EncodedKey>>,
	rownumber_evict_cursor: RefCell<Option<EncodedKey>>,
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
		left_ttl_anchor: TtlAnchor,
		right_ttl: Option<Duration>,
		right_ttl_anchor: TtlAnchor,
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
			left_ttl_anchor,
			right_ttl,
			right_ttl_anchor,
			left_evict_cursor: RefCell::new(None),
			right_evict_cursor: RefCell::new(None),
			rownumber_evict_cursor: RefCell::new(None),
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
		left_ttl_anchor: TtlAnchor,
		right_ttl: Option<Duration>,
		right_ttl_anchor: TtlAnchor,
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
			row_number_provider: RowNumberProvider::new(node),
			routines,
			runtime_context,
			snapshot: true,
			natural: false,
			latest: false,
			left_ttl,
			left_ttl_anchor,
			right_ttl,
			right_ttl_anchor,
			left_evict_cursor: RefCell::new(None),
			right_evict_cursor: RefCell::new(None),
			rownumber_evict_cursor: RefCell::new(None),
		}
	}

	fn evict_left(&self, txn: &mut FlowTransaction, now: DateTime) -> Result<()> {
		let Some(ttl) = self.left_ttl else {
			return Ok(());
		};
		let left = Store::new(self.node, JoinSide::Left);
		let mut cursor = self.left_evict_cursor.borrow_mut().take();
		left.evict_expired(txn, now, ttl, self.left_ttl_anchor, &mut cursor, EVICT_BATCH)?;
		*self.left_evict_cursor.borrow_mut() = cursor;
		Ok(())
	}

	fn evict_right(&self, txn: &mut FlowTransaction, now: DateTime) -> Result<()> {
		let Some(ttl) = self.right_ttl else {
			return Ok(());
		};
		let right = Store::new(self.node, JoinSide::Right);
		let mut cursor = self.right_evict_cursor.borrow_mut().take();
		right.evict_expired(txn, now, ttl, self.right_ttl_anchor, &mut cursor, EVICT_BATCH)?;
		*self.right_evict_cursor.borrow_mut() = cursor;
		Ok(())
	}

	fn evict_rownumbers(&self, txn: &mut FlowTransaction, now: DateTime) -> Result<()> {
		let Some(ttl) = self.left_ttl else {
			return Ok(());
		};
		let mut cursor = self.rownumber_evict_cursor.borrow_mut().take();
		self.row_number_provider.evict_expired(
			txn,
			now,
			ttl,
			self.left_ttl_anchor,
			&mut cursor,
			EVICT_BATCH,
		)?;
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
			arena: None,
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
		for row_idx in 0..row_count {
			let mut hasher = Vec::with_capacity(256);
			let mut has_undefined = false;

			for col in &expr_columns {
				let value = col.data().get_value(row_idx);

				if matches!(value, Value::None { .. }) {
					has_undefined = true;
					break;
				}

				let bytes = to_stdvec(&value).map_err(|e| {
					Error(Box::new(internal!("Failed to encode value for hash: {}", e)))
				})?;
				hasher.extend_from_slice(&bytes);
			}

			if has_undefined {
				hashes.push(None);
			} else {
				hashes.push(Some(xxh3_128(&hasher)));
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
		let left_row_number = left.row_numbers[left_idx];

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
				let left_row_number = left.row_numbers[idx];
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

		let left_row_number = left.row_numbers[left_idx];

		let composite_keys: Vec<EncodedKey> = (0..right_count)
			.map(|right_idx| {
				let right_row_number = right.row_numbers[right_idx];
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

		let right_row_number = right.row_numbers[right_idx];

		let composite_keys: Vec<EncodedKey> = (0..left_count)
			.map(|left_idx| {
				let left_row_number = left.row_numbers[left_idx];
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
			let left_row_number = left.row_numbers[left_idx];
			for &right_idx in right_indices {
				let right_row_number = right.row_numbers[right_idx];
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
		let row_numbers: Vec<RowNumber> = left_indices.iter().map(|&idx| left.row_numbers[idx]).collect();
		let builder = JoinedColumnsBuilder::new(left, slot, &self.alias, self.natural);
		builder.join_cartesian(&row_numbers, left, left_indices, slot, &[0])
	}

	pub(crate) fn unmatched_left_latest(&self, left: &Columns, left_indices: &[usize]) -> Columns {
		let row_numbers: Vec<RowNumber> = left_indices.iter().map(|&idx| left.row_numbers[idx]).collect();
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
		OperatorCapability::STANDARD_WITH_TICK
	}

	fn ticks(&self) -> Option<Duration> {
		if self.left_ttl.is_some() || self.right_ttl.is_some() {
			Some(Duration::from_seconds(1).unwrap())
		} else {
			None
		}
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		self.evict_left(txn, tick.now)?;
		if !self.latest {
			self.evict_right(txn, tick.now)?;
			self.evict_rownumbers(txn, tick.now)?;
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
				Error(Box::new(internal!("Join operator received diff from unknown node")))
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

		for (row_idx, key) in keys.iter().enumerate() {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
			};
			let diffs = match key {
				Some(key_hash) => {
					self.strategy.handle_insert(txn, post, &[row_idx], key_hash, &mut ctx)?
				}
				None => self.strategy.handle_insert_undefined(txn, post, row_idx, &mut ctx)?,
			};
			result.extend(diffs);
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

#[cfg(test)]
mod tick_tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{common::CommitVersion, encoded::row::EncodedRow};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::blob::Blob;

	use super::*;

	fn ttl(millis: i64) -> Duration {
		Duration::from_milliseconds_const(millis)
	}

	fn make_tick(engine: &TestEngine) -> Tick {
		Tick {
			now: DateTime::from_nanos(engine.clock().now_nanos()),
		}
	}

	fn make_op(
		node: u64,
		left_ttl: Option<Duration>,
		right_ttl: Option<Duration>,
		engine: &TestEngine,
	) -> JoinOperator {
		let routines = engine.executor().routines.clone();
		let rc = RuntimeContext::with_clock(engine.clock().clone());
		JoinOperator::new_for_state_tests(
			FlowNodeId(node),
			left_ttl,
			TtlAnchor::Created,
			right_ttl,
			TtlAnchor::Created,
			routines,
			rc,
		)
	}

	fn op_row(payload: u8) -> EncodedRow {
		let shape = RowShape::operator_state();
		let mut r = shape.allocate();
		shape.set_blob(&mut r, 0, &Blob::from(vec![payload]));
		r
	}

	#[test]
	fn tick_evicts_rownumbers_past_ttl() {
		// A join mints one row-number mapping per (left,right) output pair. If those mappings are
		// never evicted once the left row ages past the left TTL, the join's internal state grows
		// without bound (observed: 430M mapping rows / 66GB on a live ingestor). evict_rownumbers
		// must drop the aged mappings and keep the fresh ones.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(30, Some(ttl(50)), None, &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		let old = JoinOperator::make_composite_key(RowNumber(1), RowNumber(1));
		op.row_number_provider.get_or_create_row_number(&mut txn, &old).unwrap();

		mock_clock.advance_millis(40);
		let young = JoinOperator::make_composite_key(RowNumber(2), RowNumber(1));
		op.row_number_provider.get_or_create_row_number(&mut txn, &young).unwrap();

		mock_clock.advance_millis(20);
		let emitted = op.tick(&mut txn, make_tick(&engine)).unwrap();
		assert!(emitted.is_none(), "join tick must be silent (no downstream change)");

		assert!(
			op.row_number_provider.get_row_number(&mut txn, &old).unwrap().is_none(),
			"a mapping whose left row aged past the left TTL must be evicted"
		);
		assert!(
			op.row_number_provider.get_row_number(&mut txn, &young).unwrap().is_some(),
			"a mapping still within the left TTL window must survive"
		);
	}

	#[test]
	fn tick_evicts_left_store_past_ttl() {
		// evict_left must drop left-store rows older than the left TTL.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(30, Some(ttl(50)), None, &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		let left = Store::new(FlowNodeId(30), JoinSide::Left);
		let hash = Hash128(0xABC);
		left.put_row(&mut txn, &hash, RowNumber(1), &op_row(0x10)).unwrap();
		mock_clock.advance_millis(40);
		left.put_row(&mut txn, &hash, RowNumber(2), &op_row(0x20)).unwrap();

		mock_clock.advance_millis(20);
		op.tick(&mut txn, make_tick(&engine)).unwrap();

		let remaining = left.rows_for_key(&mut txn, &hash).unwrap();
		assert_eq!(remaining.len(), 1, "only the within-TTL left-store row survives");
		assert_eq!(remaining[0].0, RowNumber(2));
	}

	#[test]
	fn tick_evicts_right_store_past_ttl() {
		// The snapshot right store accumulates one row per churned upstream RowNumber (observed
		// ~2875 rows per hot mint, since upstream TTL drops never emit a Remove). evict_right must
		// drop right-store rows past the right TTL so the probe fan-out and storage stay bounded.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(30, None, Some(ttl(50)), &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		let right = Store::new(FlowNodeId(30), JoinSide::Right);
		let hash = Hash128(0xABC);
		right.put_row(&mut txn, &hash, RowNumber(1), &op_row(0x10)).unwrap();
		mock_clock.advance_millis(40);
		right.put_row(&mut txn, &hash, RowNumber(2), &op_row(0x20)).unwrap();

		mock_clock.advance_millis(20);
		op.tick(&mut txn, make_tick(&engine)).unwrap();

		let remaining = right.rows_for_key(&mut txn, &hash).unwrap();
		assert_eq!(remaining.len(), 1, "only the within-TTL right-store row survives");
		assert_eq!(remaining[0].0, RowNumber(2));
	}

	#[test]
	fn tick_is_noop_when_no_ttl_set() {
		// With neither side's TTL configured the join must not evict anything (mappings retained,
		// exactly as before this change; the central GC still bounds the data stores).
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(30, None, None, &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		let key = JoinOperator::make_composite_key(RowNumber(1), RowNumber(1));
		op.row_number_provider.get_or_create_row_number(&mut txn, &key).unwrap();

		mock_clock.advance_millis(10_000);
		let emitted = op.tick(&mut txn, make_tick(&engine)).unwrap();
		assert!(emitted.is_none());
		assert!(
			op.row_number_provider.get_row_number(&mut txn, &key).unwrap().is_some(),
			"with no TTL configured the tick must retain mappings"
		);
	}

	#[test]
	fn tick_preserves_row_number_counter() {
		// Evicting every mapping must NOT reset the monotonic counter; a fresh mapping after a
		// full eviction must get a strictly larger number, or a recycled id would corrupt any
		// downstream consumer that tracks rows by number.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(30, Some(ttl(50)), None, &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		let first = JoinOperator::make_composite_key(RowNumber(1), RowNumber(1));
		let (n1, _) = op.row_number_provider.get_or_create_row_number(&mut txn, &first).unwrap();

		mock_clock.advance_millis(100);
		op.tick(&mut txn, make_tick(&engine)).unwrap();
		assert!(op.row_number_provider.get_row_number(&mut txn, &first).unwrap().is_none());

		let second = JoinOperator::make_composite_key(RowNumber(7), RowNumber(7));
		let (n2, is_new) = op.row_number_provider.get_or_create_row_number(&mut txn, &second).unwrap();
		assert!(is_new);
		assert!(n2.0 > n1.0, "counter must keep advancing past evicted mappings, not recycle ids");
	}

	#[test]
	fn capabilities_always_include_tick() {
		// The engine calls enforce_tick_capability before tick() and aborts the process if Tick is
		// absent; capabilities must include Tick unconditionally, even when no TTL is set.
		let engine = TestEngine::new();
		let with = make_op(1, Some(ttl(100)), None, &engine);
		assert!(with.capabilities().contains(&OperatorCapability::Tick));
		let without = make_op(2, None, None, &engine);
		assert!(without.capabilities().contains(&OperatorCapability::Tick));
	}
}
