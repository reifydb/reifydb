// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::{Arc, LazyLock};

use postcard::to_stdvec;
use reifydb_abi::operator::capabilities::{CAPABILITY_ALL_STANDARD, CAPABILITY_TICK};
use reifydb_core::{
	common::{CommitVersion, JoinType},
	encoded::{key::EncodedKey, shape::RowShape},
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin, Diff},
	},
	internal,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
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
use reifydb_type::{
	Result,
	error::Error,
	params::Params,
	value::{Value, datetime::DateTime, identity::IdentityId, row_number::RowNumber, r#type::Type},
};

use super::{
	column::JoinedColumnsBuilder,
	state::{JoinSide, JoinState},
	strategy::{JoinContext, JoinStrategy, UpdateKeys},
};
use crate::{
	operator::{
		Operator, Operators,
		join::store::Store,
		stateful::{raw::RawStatefulOperator, row::RowNumberProvider, single::SingleStateful},
	},
	transaction::FlowTransaction,
};

#[derive(Default, Clone, Copy)]
pub struct JoinStateTtl {
	pub left_nanos: Option<u64>,
	pub right_nanos: Option<u64>,
}

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

pub struct JoinSideConfig {
	pub parent: Arc<Operators>,
	pub node: FlowNodeId,
	pub exprs: Vec<Expression>,
	pub schema: Columns,
}

pub struct JoinOperator {
	pub(crate) left_parent: Arc<Operators>,
	pub(crate) right_parent: Arc<Operators>,
	node: FlowNodeId,
	strategy: JoinStrategy,
	left_node: FlowNodeId,
	right_node: FlowNodeId,
	left_exprs: Vec<Expression>,
	pub(crate) right_exprs: Vec<Expression>,
	compiled_left_exprs: Vec<CompiledExpr>,
	compiled_right_exprs: Vec<CompiledExpr>,
	alias: Option<String>,
	shape: RowShape,
	right_schema: Columns,
	row_number_provider: RowNumberProvider,
	executor: Executor,
	routines: Routines,
	runtime_context: RuntimeContext,
	ttl: JoinStateTtl,
	pub(crate) snapshot: bool,
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
		ttl: JoinStateTtl,
		snapshot: bool,
	) -> Self {
		let left_parent = left.parent;
		let right_parent = right.parent;
		let left_node = left.node;
		let right_node = right.node;
		let left_exprs = left.exprs;
		let right_exprs = right.exprs;
		let right_schema = right.schema;
		let strategy = JoinStrategy::from(join_type);
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
			left_parent,
			right_parent,
			node,
			strategy,
			left_node,
			right_node,
			left_exprs,
			right_exprs,
			compiled_left_exprs,
			compiled_right_exprs,
			alias,
			shape,
			right_schema,
			row_number_provider,
			executor,
			routines,
			runtime_context,
			ttl,
			snapshot,
		}
	}

	fn state_shape() -> RowShape {
		RowShape::operator_state()
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
						ColumnWithName::undefined_typed(col_name, Type::Boolean, row_count)
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

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias);
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

		let builder = JoinedColumnsBuilder::new(left, &self.right_schema, &self.alias);
		Ok(builder.unmatched_left_batch(&row_numbers, left, left_indices, &self.right_schema))
	}

	pub(crate) fn cleanup_left_row_joins(&self, txn: &mut FlowTransaction, left_number: u64) -> Result<()> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_number);
		let prefix = serializer.finish();

		self.row_number_provider.remove_by_prefix(txn, &prefix)
	}

	pub(crate) fn join_columns(
		&self,
		txn: &mut FlowTransaction,
		left: &Columns,
		left_idx: usize,
		right: &Columns,
		right_idx: usize,
	) -> Result<Columns> {
		let left_row_number = left.row_numbers[left_idx];
		let right_row_number = right.row_numbers[right_idx];

		let composite_key = Self::make_composite_key(left_row_number, right_row_number);
		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias);
		Ok(builder.join_at_indices(result_row_number, left, left_idx, right, right_idx))
	}

	fn make_composite_key(left_num: RowNumber, right_num: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'L');
		serializer.extend_u64(left_num.0);
		serializer.extend_u64(right_num.0);
		serializer.finish()
	}

	fn parse_composite_key(key_bytes: &[u8]) -> Option<(RowNumber, Option<RowNumber>)> {
		if key_bytes.is_empty() || key_bytes[0] != !b'L' {
			return None;
		}

		let mut de = KeyDeserializer::from_bytes(&key_bytes[1..]);
		let left_num = de.read_u64().ok()?;
		let right_num = de.read_u64().ok().map(RowNumber);

		Some((RowNumber(left_num), right_num))
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

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias);
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

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias);
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

		let builder = JoinedColumnsBuilder::new(left, right, &self.alias);
		Ok(builder.join_cartesian(&row_numbers, left, left_indices, right, right_indices))
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

	fn capabilities(&self) -> u32 {
		CAPABILITY_ALL_STANDARD | CAPABILITY_TICK
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		if let ChangeOrigin::Flow(from_node) = &change.origin
			&& *from_node == self.node
		{
			return Ok(Change::from_flow(self.node, change.version, Vec::new(), DateTime::default()));
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
				} => self.apply_join_insert(
					txn,
					&post,
					compiled_exprs,
					side,
					version,
					&mut state,
					&mut result,
				)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_join_remove(
					txn,
					&pre,
					compiled_exprs,
					side,
					version,
					&mut state,
					&mut result,
				)?,
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
					version,
					&mut state,
					&mut result,
				)?,
			}
		}

		Ok(Change::from_flow(self.node, version, result, change.changed_at))
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		evict_per_side_ttl(txn, self.node, self.ttl, tick.now.to_nanos())?;
		Ok(None)
	}
}

pub(crate) fn evict_per_side_ttl(
	txn: &mut FlowTransaction,
	node: FlowNodeId,
	ttl: JoinStateTtl,
	now_nanos: u64,
) -> Result<()> {
	if let Some(ttl_nanos) = ttl.left_nanos {
		let cutoff = now_nanos.saturating_sub(ttl_nanos);
		Store::new(node, JoinSide::Left).tick_evict(txn, cutoff)?;
	}
	if let Some(ttl_nanos) = ttl.right_nanos {
		let cutoff = now_nanos.saturating_sub(ttl_nanos);
		Store::new(node, JoinSide::Right).tick_evict(txn, cutoff)?;
	}
	Ok(())
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
		version: CommitVersion,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(post, compiled_exprs)?;

		for (row_idx, key) in keys.iter().enumerate() {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
				version,
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
		version: CommitVersion,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(pre, compiled_exprs)?;

		for (row_idx, key) in keys.iter().enumerate() {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
				version,
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
		version: CommitVersion,
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
				version,
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
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{
		common::CommitVersion,
		encoded::{row::EncodedRow, shape::RowShape},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::hash::Hash128;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::value::{blob::Blob, identity::IdentityId};

	use super::*;

	fn row(payload: u8) -> EncodedRow {
		let shape = RowShape::operator_state();
		let mut r = shape.allocate();
		shape.set_blob(&mut r, 0, &Blob::from(vec![payload]));
		r
	}

	fn h(v: u128) -> Hash128 {
		Hash128(v)
	}

	fn rn(v: u64) -> RowNumber {
		RowNumber(v)
	}

	fn put(store: &Store, txn: &mut FlowTransaction, hash: u128, row_number: u64, payload: u8) {
		store.put_row(txn, &h(hash), rn(row_number), &row(payload)).unwrap();
	}

	fn contains(store: &Store, txn: &mut FlowTransaction, hash: u128) -> bool {
		store.contains_key(txn, &h(hash)).unwrap()
	}

	#[test]
	fn tick_with_no_ttl_is_noop() {
		// When neither side has a TTL configured, advancing the clock arbitrarily must not
		// cause eviction. A regression that defaulted to "evict everything older than now"
		// would silently wipe state across both sides.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(101);
		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);

		put(&left, &mut txn, 0xAAA, 1, 0x10);
		put(&right, &mut txn, 0xBBB, 2, 0x20);

		mock_clock.advance_millis(10_000);

		evict_per_side_ttl(&mut txn, node, JoinStateTtl::default(), engine.clock().now_nanos()).unwrap();

		assert!(contains(&left, &mut txn, 0xAAA), "left side must keep its row when no TTL is set");
		assert!(contains(&right, &mut txn, 0xBBB), "right side must keep its row when no TTL is set");
	}

	#[test]
	fn tick_evicts_only_left_when_right_ttl_is_none() {
		// Verifies that left.ttl is applied to the left store and that the right store is
		// untouched. The most common regression here would be the right branch reading the
		// left's cutoff (or vice versa).
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(102);
		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);

		put(&left, &mut txn, 0xAAA, 1, 0x10);
		put(&right, &mut txn, 0xBBB, 2, 0x20);

		mock_clock.advance_millis(50);

		let ttl = JoinStateTtl {
			left_nanos: Some(30_000_000), // 30ms
			right_nanos: None,
		};
		evict_per_side_ttl(&mut txn, node, ttl, engine.clock().now_nanos()).unwrap();

		assert!(!contains(&left, &mut txn, 0xAAA), "left side must be evicted past its TTL");
		assert!(contains(&right, &mut txn, 0xBBB), "right side must keep its row when right_nanos is None");
	}

	#[test]
	fn tick_evicts_only_right_when_left_ttl_is_none() {
		// Mirror of the previous test: this would catch a regression that hardcoded the side
		// being evicted, or that copied left.ttl across both branches.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(103);
		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);

		put(&left, &mut txn, 0xAAA, 1, 0x10);
		put(&right, &mut txn, 0xBBB, 2, 0x20);

		mock_clock.advance_millis(50);

		let ttl = JoinStateTtl {
			left_nanos: None,
			right_nanos: Some(30_000_000), // 30ms
		};
		evict_per_side_ttl(&mut txn, node, ttl, engine.clock().now_nanos()).unwrap();

		assert!(contains(&left, &mut txn, 0xAAA), "left side must keep its row when left_nanos is None");
		assert!(!contains(&right, &mut txn, 0xBBB), "right side must be evicted past its TTL");
	}

	#[test]
	fn tick_with_symmetric_ttl_evicts_both_sides_after_cutoff() {
		// Both sides configured with the same TTL: after the cutoff, both must be evicted.
		// A regression that silently skipped one side would fail this.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(104);
		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);

		put(&left, &mut txn, 0xAAA, 1, 0x10);
		put(&right, &mut txn, 0xBBB, 2, 0x20);

		mock_clock.advance_millis(60);

		let ttl = JoinStateTtl {
			left_nanos: Some(50_000_000), // 50ms
			right_nanos: Some(50_000_000),
		};
		evict_per_side_ttl(&mut txn, node, ttl, engine.clock().now_nanos()).unwrap();

		assert!(!contains(&left, &mut txn, 0xAAA), "left side must be evicted past the symmetric TTL");
		assert!(!contains(&right, &mut txn, 0xBBB), "right side must be evicted past the symmetric TTL");
	}

	#[test]
	fn tick_with_asymmetric_ttl_evicts_left_first_then_right() {
		// The headline test for per-side TTL: each side evicts on its own clock. Critically
		// detects the most likely failure mode of feeding both sides the same cutoff. We
		// drive two ticks at different times to show left dies first while right survives,
		// then right dies after its own (longer) TTL.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(105);
		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);

		put(&left, &mut txn, 0xAAA, 1, 0x10);
		put(&right, &mut txn, 0xBBB, 2, 0x20);

		let ttl = JoinStateTtl {
			left_nanos: Some(30_000_000),  // 30ms
			right_nanos: Some(90_000_000), // 90ms
		};

		mock_clock.advance_millis(40);
		evict_per_side_ttl(&mut txn, node, ttl, engine.clock().now_nanos()).unwrap();
		assert!(!contains(&left, &mut txn, 0xAAA), "left side must be evicted past its 30ms TTL");
		assert!(contains(&right, &mut txn, 0xBBB), "right side must survive while still within its 90ms TTL");

		mock_clock.advance_millis(60);
		evict_per_side_ttl(&mut txn, node, ttl, engine.clock().now_nanos()).unwrap();
		assert!(!contains(&right, &mut txn, 0xBBB), "right side must be evicted past its 90ms TTL");
	}

	#[test]
	fn tick_evicts_per_row_not_per_side() {
		// Inside a single side, a row inserted later must survive even when an older row in
		// the same side gets evicted. Catches a regression that wipes the whole side based
		// on the oldest row or the first scanned row.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(106);
		let left = Store::new(node, JoinSide::Left);

		put(&left, &mut txn, 0xAAA, 1, 0x10);
		mock_clock.advance_millis(30);
		put(&left, &mut txn, 0xBBB, 2, 0x20);

		mock_clock.advance_millis(30);
		// now = 60ms, ttl = 50ms, cutoff = 10ms.
		// AAA inserted at 0ms (stale), BBB inserted at 30ms (within window).
		let ttl = JoinStateTtl {
			left_nanos: Some(50_000_000),
			right_nanos: None,
		};
		evict_per_side_ttl(&mut txn, node, ttl, engine.clock().now_nanos()).unwrap();

		assert!(!contains(&left, &mut txn, 0xAAA), "older row must be evicted");
		assert!(contains(&left, &mut txn, 0xBBB), "younger row in the same side must survive");
	}

	#[test]
	fn tick_does_not_evict_rows_within_ttl_window() {
		// Off-by-one check on the cutoff math: rows whose age is below the TTL must remain.
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(107);
		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);

		put(&left, &mut txn, 0xAAA, 1, 0x10);
		put(&right, &mut txn, 0xBBB, 2, 0x20);

		mock_clock.advance_millis(50);

		let ttl = JoinStateTtl {
			left_nanos: Some(100_000_000), // 100ms
			right_nanos: Some(100_000_000),
		};
		evict_per_side_ttl(&mut txn, node, ttl, engine.clock().now_nanos()).unwrap();

		assert!(contains(&left, &mut txn, 0xAAA), "left must survive while under its TTL");
		assert!(contains(&right, &mut txn, 0xBBB), "right must survive while under its TTL");
	}

	#[test]
	fn tick_is_safe_when_no_state_was_written() {
		// Calling tick before any insert must not panic or error. Catches an unguarded scan
		// or decode against an empty range, which would surface as a runtime failure during
		// flow startup.
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(108);

		let ttl = JoinStateTtl {
			left_nanos: Some(10_000_000),
			right_nanos: Some(10_000_000),
		};
		evict_per_side_ttl(&mut txn, node, ttl, engine.clock().now_nanos()).unwrap();

		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);
		assert!(!contains(&left, &mut txn, 0xAAA));
		assert!(!contains(&right, &mut txn, 0xAAA));
	}
}
