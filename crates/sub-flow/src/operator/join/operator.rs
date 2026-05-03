// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use indexmap::IndexMap;
use postcard::to_stdvec;
use reifydb_core::{
	common::{CommitVersion, JoinType},
	encoded::{key::EncodedKey, shape::RowShape},
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin, Diff},
	},
	internal,
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
use reifydb_type::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	util::cowvec::CowVec,
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
	row_number_provider: RowNumberProvider,
	executor: Executor,
	routines: Routines,
	runtime_context: RuntimeContext,
	ttl: JoinStateTtl,
}

impl JoinOperator {
	pub fn new(
		left: JoinSideConfig,
		right: JoinSideConfig,
		node: FlowNodeId,
		join_type: JoinType,
		alias: Option<String>,
		executor: Executor,
		ttl: JoinStateTtl,
	) -> Self {
		let left_parent = left.parent;
		let right_parent = right.parent;
		let left_node = left.node;
		let right_node = right.node;
		let left_exprs = left.exprs;
		let right_exprs = right.exprs;
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
			row_number_provider,
			executor,
			routines,
			runtime_context,
			ttl,
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
		let composite_key = EncodedKey::new(serializer.finish());

		let (result_row_number, _is_new) =
			self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;

		let right_shape = self.right_parent.pull(txn, &[])?;

		let builder = JoinedColumnsBuilder::new(left, &right_shape, &self.alias);
		Ok(builder.unmatched_left(result_row_number, left, left_idx, &right_shape))
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
				EncodedKey::new(serializer.finish())
			})
			.collect();

		let row_numbers_with_flags =
			self.row_number_provider.get_or_create_row_numbers(txn, composite_keys.iter())?;
		let row_numbers: Vec<RowNumber> = row_numbers_with_flags.iter().map(|(rn, _)| *rn).collect();

		let right_shape = self.right_parent.pull(txn, &[])?;

		let builder = JoinedColumnsBuilder::new(left, &right_shape, &self.alias);
		Ok(builder.unmatched_left_batch(&row_numbers, left, left_indices, &right_shape))
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
		EncodedKey::new(serializer.finish())
	}

	fn decode_row_number_from_keycode(bytes: &[u8]) -> u64 {
		let arr: [u8; 8] =
			[!bytes[0], !bytes[1], !bytes[2], !bytes[3], !bytes[4], !bytes[5], !bytes[6], !bytes[7]];
		u64::from_be_bytes(arr)
	}

	fn parse_composite_key(key_bytes: &[u8]) -> Option<(RowNumber, Option<RowNumber>)> {
		if key_bytes.len() < 9 || key_bytes[0] != !b'L' {
			return None;
		}

		let left_num = Self::decode_row_number_from_keycode(&key_bytes[1..9]);
		let right_num = if key_bytes.len() >= 17 {
			Some(RowNumber(Self::decode_row_number_from_keycode(&key_bytes[9..17])))
		} else {
			None
		};

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

	fn determine_side(&self, change: &Change) -> Option<JoinSide> {
		match &change.origin {
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

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		if let ChangeOrigin::Flow(from_node) = &change.origin
			&& *from_node == self.node
		{
			return Ok(Change::from_flow(self.node, change.version, Vec::new(), DateTime::default()));
		}

		let mut state = JoinState::new(self.node);
		let mut result = Vec::with_capacity(change.diffs.len() * 2);

		let side = self
			.determine_side(&change)
			.ok_or_else(|| Error(Box::new(internal!("Join operator received change from unknown node"))))?;

		let compiled_exprs = match side {
			JoinSide::Left => &self.compiled_left_exprs,
			JoinSide::Right => &self.compiled_right_exprs,
		};

		let version = change.version;
		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
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
		let now_nanos = tick.now.to_nanos();

		if let Some(ttl_nanos) = self.ttl.left_nanos {
			let cutoff = now_nanos.saturating_sub(ttl_nanos);
			let store = Store::new(self.node, JoinSide::Left);
			store.tick_evict(txn, cutoff)?;
		}
		if let Some(ttl_nanos) = self.ttl.right_nanos {
			let cutoff = now_nanos.saturating_sub(ttl_nanos);
			let store = Store::new(self.node, JoinSide::Right);
			store.tick_evict(txn, cutoff)?;
		}

		Ok(None)
	}

	// FIXME #244 The issue is that when we need to reconstruct an unmatched left row, we need the right side's

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		let mut found_columns: Vec<Columns> = Vec::new();

		for &row_number in rows {
			if let Some(joined) = self.pull_one_joined_row(txn, row_number)? {
				found_columns.push(joined);
			}
		}

		if found_columns.is_empty() {
			return self.empty_joined_shape(txn);
		}
		if found_columns.len() == 1 {
			return Ok(found_columns.remove(0));
		}

		let mut result = found_columns.remove(0);
		for cols in found_columns {
			result.row_numbers.make_mut().extend(cols.row_numbers.iter().copied());
			for (i, col) in cols.columns.into_iter().enumerate() {
				result.columns.make_mut()[i].extend(col).expect("shape mismatch in join pull");
			}
		}
		Ok(result)
	}
}

impl JoinOperator {
	#[inline]
	fn pull_one_joined_row(&self, txn: &mut FlowTransaction, row_number: RowNumber) -> Result<Option<Columns>> {
		let Some(key) = self.row_number_provider.get_key_for_row_number(txn, row_number)? else {
			return Ok(None);
		};
		let Some((left_row_number, right_row_number)) = Self::parse_composite_key(key.as_ref()) else {
			return Ok(None);
		};

		let left_cols = self.left_parent.pull(txn, &[left_row_number])?;
		if left_cols.is_empty() {
			return Ok(None);
		}

		if let Some(right_row_num) = right_row_number {
			let right_cols = self.right_parent.pull(txn, &[right_row_num])?;
			if right_cols.is_empty() {
				return Ok(None);
			}
			let builder = JoinedColumnsBuilder::new(&left_cols, &right_cols, &self.alias);
			let mut joined = builder.join_single(row_number, &left_cols, &right_cols);
			joined.row_numbers = CowVec::new(vec![row_number]);
			Ok(Some(joined))
		} else {
			let right_shape = self.right_parent.pull(txn, &[])?;
			let builder = JoinedColumnsBuilder::new(&left_cols, &right_shape, &self.alias);
			let mut unmatched = builder.unmatched_left(row_number, &left_cols, 0, &right_shape);
			unmatched.row_numbers = CowVec::new(vec![row_number]);
			Ok(Some(unmatched))
		}
	}

	#[inline]
	fn empty_joined_shape(&self, txn: &mut FlowTransaction) -> Result<Columns> {
		let left_shape = self.left_parent.pull(txn, &[])?;
		let right_shape = self.right_parent.pull(txn, &[])?;
		let builder = JoinedColumnsBuilder::new(&left_shape, &right_shape, &self.alias);
		let right_names = builder.right_column_names();

		let mut all_columns: Vec<ColumnWithName> = left_shape
			.names
			.iter()
			.zip(left_shape.columns.iter())
			.map(|(name, data)| ColumnWithName::new(name.clone(), data.clone()))
			.collect();

		for (col, aliased_name) in right_shape.columns.into_iter().zip(right_names.iter()) {
			all_columns.push(ColumnWithName::new(Fragment::internal(aliased_name), col));
		}

		Ok(Columns::new(all_columns))
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
		version: CommitVersion,
		state: &mut JoinState,
		result: &mut Vec<Diff>,
	) -> Result<()> {
		let keys = self.compute_join_keys(post, compiled_exprs)?;
		let mut inserts_by_key: IndexMap<Hash128, Vec<usize>> = IndexMap::new();
		let mut inserts_undefined: Vec<usize> = Vec::new();

		for (row_idx, key) in keys.iter().enumerate() {
			if let Some(key_hash) = key {
				inserts_by_key.entry(*key_hash).or_default().push(row_idx);
			} else {
				inserts_undefined.push(row_idx);
			}
		}

		for (key_hash, indices) in inserts_by_key {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
				version,
			};
			let diffs = self.strategy.handle_insert(txn, post, &indices, &key_hash, &mut ctx)?;
			result.extend(diffs);
		}

		for idx in inserts_undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
				version,
			};
			let diffs = self.strategy.handle_insert_undefined(txn, post, idx, &mut ctx)?;
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
		let mut removes_by_key: IndexMap<Hash128, Vec<usize>> = IndexMap::new();
		let mut removes_undefined: Vec<usize> = Vec::new();

		for (row_idx, key) in keys.iter().enumerate() {
			if let Some(key_hash) = key {
				removes_by_key.entry(*key_hash).or_default().push(row_idx);
			} else {
				removes_undefined.push(row_idx);
			}
		}

		for (key_hash, indices) in removes_by_key {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
				version,
			};
			let diffs = self.strategy.handle_remove(txn, pre, &indices, &key_hash, &mut ctx)?;
			result.extend(diffs);
		}

		for idx in removes_undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
				version,
			};
			let diffs = self.strategy.handle_remove_undefined(txn, pre, idx, &mut ctx)?;
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

		let mut updates_by_key: IndexMap<(Hash128, Hash128), Vec<usize>> = IndexMap::new();
		let mut updates_undefined: Vec<usize> = Vec::new();

		for row_idx in 0..row_count {
			match (pre_keys[row_idx], post_keys[row_idx]) {
				(Some(pre_key), Some(post_key)) => {
					updates_by_key.entry((pre_key, post_key)).or_default().push(row_idx);
				}
				_ => {
					updates_undefined.push(row_idx);
				}
			}
		}

		for ((pre_key, post_key), indices) in updates_by_key {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
				version,
			};
			let keys = UpdateKeys {
				pre: &pre_key,
				post: &post_key,
			};
			let diffs = self.strategy.handle_update(txn, pre, post, &indices, keys, &mut ctx)?;
			result.extend(diffs);
		}

		for row_idx in updates_undefined {
			let mut ctx = JoinContext {
				side,
				state,
				operator: self,
				version,
			};
			let diffs = self.strategy.handle_update_undefined(txn, pre, post, row_idx, &mut ctx)?;
			result.extend(diffs);
		}

		Ok(())
	}
}
