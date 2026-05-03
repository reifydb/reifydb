// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, LazyLock};

use indexmap::IndexMap;
use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	encoded::shape::RowShape,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	internal,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
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
	value::{Value, blob::Blob, datetime::DateTime, identity::IdentityId, row_number::RowNumber, r#type::Type},
};
use serde::{Deserialize, Serialize};

use crate::{
	operator::{
		Operator, Operators,
		stateful::{raw::RawStatefulOperator, single::SingleStateful, utils},
	},
	transaction::{FlowTransaction, slot::PersistFn},
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctLayout {
	names: Vec<String>,
	types: Vec<Type>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedRow {
	number: RowNumber,
	created_at: DateTime,
	updated_at: DateTime,

	#[serde(with = "serde_bytes")]
	values_bytes: Vec<u8>,
}

impl SerializedRow {
	fn from_columns_at_index(columns: &Columns, row_idx: usize) -> Self {
		let number = columns.row_numbers[row_idx];
		let created_at = if columns.created_at.is_empty() {
			DateTime::default()
		} else {
			columns.created_at[row_idx]
		};
		let updated_at = if columns.updated_at.is_empty() {
			DateTime::default()
		} else {
			columns.updated_at[row_idx]
		};

		let values: Vec<Value> = columns.iter().map(|c| c.data().get_value(row_idx)).collect();

		let values_bytes = to_stdvec(&values).expect("Failed to serialize column values");

		Self {
			number,
			created_at,
			updated_at,
			values_bytes,
		}
	}

	fn to_columns(&self, layout: &DistinctLayout) -> Columns {
		let values: Vec<Value> = from_bytes(&self.values_bytes).expect("Failed to deserialize column values");

		let mut columns_vec = Vec::with_capacity(layout.names.len());
		for (i, (name, typ)) in layout.names.iter().zip(layout.types.iter()).enumerate() {
			let value = values.get(i).cloned().unwrap_or(Value::none());
			let mut col_data = ColumnBuffer::with_capacity(typ.clone(), 1);
			col_data.push_value(value);
			columns_vec.push(ColumnWithName::new(Fragment::internal(name), col_data));
		}

		Columns::with_system_columns(
			columns_vec,
			vec![self.number],
			vec![self.created_at],
			vec![self.updated_at],
		)
	}
}

impl DistinctLayout {
	fn new() -> Self {
		Self {
			names: Vec::new(),
			types: Vec::new(),
		}
	}

	fn update_from_columns(&mut self, columns: &Columns) {
		if columns.is_empty() {
			return;
		}

		let names: Vec<String> = columns.iter().map(|c| c.name().text().to_string()).collect();
		let types: Vec<Type> = columns.iter().map(|c| c.data().get_type()).collect();

		if self.names.is_empty() {
			self.names = names;
			self.types = types;
			return;
		}

		for (i, new_type) in types.iter().enumerate() {
			if i < self.types.len() {
				if !self.types[i].is_option() && new_type.is_option() {
					self.types[i] = new_type.clone();
				}
			} else {
				self.types.push(new_type.clone());
				if i < names.len() {
					self.names.push(names[i].clone());
				}
			}
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctEntry {
	count: usize,

	first_row: SerializedRow,

	last_seen_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DistinctState {
	entries: IndexMap<Hash128, DistinctEntry>,

	layout: DistinctLayout,
}

impl Default for DistinctState {
	fn default() -> Self {
		Self {
			entries: IndexMap::new(),
			layout: DistinctLayout::new(),
		}
	}
}

pub struct DistinctOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	compiled_expressions: Vec<CompiledExpr>,
	shape: RowShape,
	routines: Routines,
	runtime_context: RuntimeContext,

	ttl_nanos: Option<u64>,
}

impl DistinctOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		expressions: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		ttl_nanos: Option<u64>,
	) -> Self {
		let symbols = SymbolTable::new();
		let compile_ctx = CompileContext {
			symbols: &symbols,
		};
		let compiled_expressions: Vec<CompiledExpr> = expressions
			.iter()
			.map(|e| compile_expression(&compile_ctx, e))
			.collect::<Result<Vec<_>>>()
			.expect("Failed to compile expressions");

		Self {
			parent,
			node,
			compiled_expressions,
			shape: RowShape::operator_state(),
			routines,
			runtime_context,
			ttl_nanos,
		}
	}

	fn compute_hashes(&self, columns: &Columns) -> Result<Vec<Hash128>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.compiled_expressions.is_empty() {
			let mut hashes = Vec::with_capacity(row_count);
			for row_idx in 0..row_count {
				let mut data = Vec::new();
				for col in columns.iter() {
					let value = col.data().get_value(row_idx);
					let value_str = value.to_string();
					data.extend_from_slice(value_str.as_bytes());
				}
				hashes.push(xxh3_128(&data));
			}
			Ok(hashes)
		} else {
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
			let mut expr_columns = Vec::new();
			for compiled_expr in &self.compiled_expressions {
				let col = compiled_expr.execute(&exec_ctx)?;
				expr_columns.push(col);
			}

			let mut hashes = Vec::with_capacity(row_count);
			for row_idx in 0..row_count {
				let mut data = Vec::new();
				for col in &expr_columns {
					let value = col.data().get_value(row_idx);
					let value_str = value.to_string();
					data.extend_from_slice(value_str.as_bytes());
				}
				hashes.push(xxh3_128(&data));
			}
			Ok(hashes)
		}
	}

	fn load_distinct_state(&self, txn: &mut FlowTransaction) -> Result<DistinctState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(DistinctState::default());
		}

		let blob = self.shape.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(DistinctState::default());
		}

		from_bytes(blob.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize DistinctState: {}", e))))
	}

	fn save_distinct_state(&self, txn: &mut FlowTransaction, state: &DistinctState) -> Result<()> {
		let serialized = to_stdvec(state)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize DistinctState: {}", e))))?;
		let blob = Blob::from(serialized);

		self.update_state(txn, |shape, row| {
			shape.set_blob(row, 0, &blob);
			Ok(())
		})?;
		Ok(())
	}

	fn process_insert(&self, state: &mut DistinctState, columns: &Columns) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		state.layout.update_from_columns(columns);
		let hashes = self.compute_hashes(columns)?;
		let now_nanos = self.runtime_context.clock.now_nanos();

		let mut new_distinct_indices: Vec<usize> = Vec::new();

		for (row_idx, &hash) in hashes.iter().enumerate() {
			match state.entries.get_mut(&hash) {
				Some(entry) => {
					entry.count += 1;
					entry.last_seen_nanos = now_nanos;
				}
				None => {
					state.entries.insert(
						hash,
						DistinctEntry {
							count: 1,
							first_row: SerializedRow::from_columns_at_index(
								columns, row_idx,
							),
							last_seen_nanos: now_nanos,
						},
					);
					new_distinct_indices.push(row_idx);
				}
			}
		}

		if !new_distinct_indices.is_empty() {
			let output = columns.extract_by_indices(&new_distinct_indices);
			result.push(Diff::insert(output));
		}

		Ok(result)
	}

	fn process_update(
		&self,
		state: &mut DistinctState,
		pre_columns: &Columns,
		post_columns: &Columns,
	) -> Result<Vec<Diff>> {
		let row_count = post_columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		state.layout.update_from_columns(post_columns);
		let pre_hashes = self.compute_hashes(pre_columns)?;
		let post_hashes = self.compute_hashes(post_columns)?;
		let now_nanos = self.runtime_context.clock.now_nanos();

		let mut same_key_update_indices: Vec<usize> = Vec::new();
		let mut removed_indices: Vec<usize> = Vec::new();
		let mut inserted_indices: Vec<usize> = Vec::new();

		for row_idx in 0..row_count {
			let pre_hash = pre_hashes[row_idx];
			let post_hash = post_hashes[row_idx];

			if pre_hash == post_hash {
				update_same_distinct_key(
					state,
					pre_hash,
					post_columns,
					row_idx,
					now_nanos,
					&mut same_key_update_indices,
				);
			} else {
				if drop_pre_distinct_key(state, pre_hash) {
					removed_indices.push(row_idx);
				}
				if add_post_distinct_key(state, post_hash, post_columns, row_idx, now_nanos) {
					inserted_indices.push(row_idx);
				}
			}
		}

		let mut result = Vec::new();
		if !same_key_update_indices.is_empty() {
			let pre_output = pre_columns.extract_by_indices(&same_key_update_indices);
			let post_output = post_columns.extract_by_indices(&same_key_update_indices);
			result.push(Diff::update(pre_output, post_output));
		}
		if !removed_indices.is_empty() {
			result.push(Diff::remove(pre_columns.extract_by_indices(&removed_indices)));
		}
		if !inserted_indices.is_empty() {
			result.push(Diff::insert(post_columns.extract_by_indices(&inserted_indices)));
		}
		Ok(result)
	}

	fn process_remove(&self, state: &mut DistinctState, columns: &Columns) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		let hashes = self.compute_hashes(columns)?;

		let mut removed_hashes: Vec<Hash128> = Vec::new();

		for &hash in &hashes {
			if let Some(entry) = state.entries.get_mut(&hash) {
				if entry.count > 1 {
					entry.count -= 1;
				} else {
					removed_hashes.push(hash);
				}
			}
		}

		for hash in removed_hashes {
			if let Some(entry) = state.entries.shift_remove(&hash) {
				let stored_columns = entry.first_row.to_columns(&state.layout);
				result.push(Diff::remove(stored_columns));
			}
		}

		Ok(result)
	}
}

#[inline]
fn update_same_distinct_key(
	state: &mut DistinctState,
	hash: Hash128,
	post_columns: &Columns,
	row_idx: usize,
	now_nanos: u64,
	indices: &mut Vec<usize>,
) {
	if let Some(entry) = state.entries.get_mut(&hash) {
		if entry.first_row.number == post_columns.row_numbers[row_idx] {
			entry.first_row = SerializedRow::from_columns_at_index(post_columns, row_idx);
		}
		entry.last_seen_nanos = now_nanos;
		indices.push(row_idx);
	}
}

#[inline]
fn drop_pre_distinct_key(state: &mut DistinctState, hash: Hash128) -> bool {
	let Some(entry) = state.entries.get_mut(&hash) else {
		return false;
	};
	if entry.count > 1 {
		entry.count -= 1;
		false
	} else {
		state.entries.shift_remove(&hash);
		true
	}
}

#[inline]
fn add_post_distinct_key(
	state: &mut DistinctState,
	hash: Hash128,
	post_columns: &Columns,
	row_idx: usize,
	now_nanos: u64,
) -> bool {
	match state.entries.get_mut(&hash) {
		Some(entry) => {
			entry.count += 1;
			entry.last_seen_nanos = now_nanos;
			false
		}
		None => {
			state.entries.insert(
				hash,
				DistinctEntry {
					count: 1,
					first_row: SerializedRow::from_columns_at_index(post_columns, row_idx),
					last_seen_nanos: now_nanos,
				},
			);
			true
		}
	}
}

impl RawStatefulOperator for DistinctOperator {}

impl SingleStateful for DistinctOperator {
	fn layout(&self) -> RowShape {
		self.shape.clone()
	}
}

impl Operator for DistinctOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let node_id = self.node;
		let shape = self.shape.clone();

		let state: &mut DistinctState = txn.operator_state(node_id, |txn| {
			let s = self.load_distinct_state(txn)?;
			let persist: PersistFn = Box::new(move |txn, value| {
				let state = value.downcast::<DistinctState>().expect("DistinctState slot type");
				let serialized = to_stdvec(&*state).map_err(|e| {
					Error(Box::new(internal!("Failed to serialize DistinctState: {}", e)))
				})?;
				let blob = Blob::from(serialized);
				let key = utils::empty_key();
				let mut row = utils::load_or_create_row(node_id, txn, &key, &shape)?;
				shape.set_blob(&mut row, 0, &blob);
				utils::save_row(node_id, txn, &key, row)?;
				Ok(())
			});
			Ok((s, persist))
		})?;

		let mut result = Vec::new();
		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => {
					let insert_result = self.process_insert(state, &post)?;
					result.extend(insert_result);
				}
				Diff::Update {
					pre,
					post,
				} => {
					let update_result = self.process_update(state, &pre, &post)?;
					result.extend(update_result);
				}
				Diff::Remove {
					pre,
				} => {
					let remove_result = self.process_remove(state, &pre)?;
					result.extend(remove_result);
				}
			}
		}

		txn.mark_state_dirty(node_id);

		Ok(Change::from_flow(self.node, change.version, result, change.changed_at))
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let Some(ttl_nanos) = self.ttl_nanos else {
			return Ok(None);
		};
		let cutoff = tick.now.to_nanos().saturating_sub(ttl_nanos);

		let node_id = self.node;
		let shape = self.shape.clone();

		let state: &mut DistinctState = txn.operator_state(node_id, |txn| {
			let s = self.load_distinct_state(txn)?;
			let persist: PersistFn = Box::new(move |txn, value| {
				let state = value.downcast::<DistinctState>().expect("DistinctState slot type");
				let serialized = to_stdvec(&*state).map_err(|e| {
					Error(Box::new(internal!("Failed to serialize DistinctState: {}", e)))
				})?;
				let blob = Blob::from(serialized);
				let key = utils::empty_key();
				let mut row = utils::load_or_create_row(node_id, txn, &key, &shape)?;
				shape.set_blob(&mut row, 0, &blob);
				utils::save_row(node_id, txn, &key, row)?;
				Ok(())
			});
			Ok((s, persist))
		})?;

		let initial = state.entries.len();
		state.entries.retain(|_, entry| entry.last_seen_nanos >= cutoff);
		let evicted = initial - state.entries.len();

		if evicted > 0 {
			txn.mark_state_dirty(node_id);
		}

		Ok(None)
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		self.parent.pull(txn, rows)
	}
}

#[cfg(test)]
mod ttl_tests {
	use std::sync::Arc as StdArc;

	use reifydb_core::{
		common::CommitVersion,
		interface::change::{Change, Diff, Diffs},
		value::column::{ColumnWithName, buffer::ColumnBuffer},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::{
		fragment::Fragment,
		util::cowvec::CowVec,
		value::{container::number::NumberContainer, identity::IdentityId},
	};

	use super::*;

	struct NoOpParent;

	impl Operator for NoOpParent {
		fn id(&self) -> FlowNodeId {
			FlowNodeId(0)
		}

		fn apply(&self, _: &mut FlowTransaction, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn pull(&self, _: &mut FlowTransaction, _: &[RowNumber]) -> Result<Columns> {
			Ok(Columns::empty())
		}
	}

	fn build_insert(value: i64, row_num: u64) -> Change {
		let cols = vec![ColumnWithName::new(
			Fragment::internal("k"),
			ColumnBuffer::Int8(NumberContainer::from_parts(CowVec::new(vec![value]))),
		)];
		let now = DateTime::default();
		let columns = Columns::with_system_columns(cols, vec![RowNumber(row_num)], vec![now], vec![now]);
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns));
		Change::from_flow(FlowNodeId(99), CommitVersion(1), diffs, now)
	}

	fn make_op(node_id: u64, ttl_nanos: Option<u64>, engine: &TestEngine) -> DistinctOperator {
		let routines = engine.executor().routines.clone();
		let rc = RuntimeContext::with_clock(engine.clock().clone());
		let parent: StdArc<Operators> = StdArc::new(Operators::Custom(Box::new(NoOpParent)));
		DistinctOperator::new(parent, FlowNodeId(node_id), Vec::new(), routines, rc, ttl_nanos)
	}

	#[test]
	fn tick_is_noop_when_retention_is_unset() {
		let engine = TestEngine::new();
		let op = make_op(1, None, &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			engine.catalog(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		op.apply(&mut txn, build_insert(42, 1)).unwrap();
		op.apply(&mut txn, build_insert(43, 2)).unwrap();

		let result = op
			.tick(
				&mut txn,
				Tick {
					now: DateTime::from_nanos(u64::MAX),
				},
			)
			.unwrap();
		assert!(result.is_none(), "tick must return Ok(None) (silent)");

		txn.flush_operator_states().unwrap();
		let state = op.load_distinct_state(&mut txn).unwrap();
		assert_eq!(state.entries.len(), 2, "no eviction when ttl is None");
	}

	#[test]
	fn tick_evicts_only_entries_past_cutoff() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		// 10ms row
		let op = make_op(2, Some(10_000_000), &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			engine.catalog(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		// Insert two entries at t = 1000ms
		op.apply(&mut txn, build_insert(42, 1)).unwrap();
		op.apply(&mut txn, build_insert(43, 2)).unwrap();

		// Advance to t = 1005ms (5ms < 10ms row) - tick must NOT evict
		mock_clock.advance_millis(5);
		let result = op
			.tick(
				&mut txn,
				Tick {
					now: DateTime::from_nanos(mock_clock.now_nanos()),
				},
			)
			.unwrap();
		assert!(result.is_none());
		txn.flush_operator_states().unwrap();
		assert_eq!(op.load_distinct_state(&mut txn).unwrap().entries.len(), 2);

		// Advance to t = 1020ms (20ms > 10ms row) - tick must evict both
		mock_clock.advance_millis(15);
		let result = op
			.tick(
				&mut txn,
				Tick {
					now: DateTime::from_nanos(mock_clock.now_nanos()),
				},
			)
			.unwrap();
		assert!(result.is_none(), "eviction is silent (Drop mode)");
		txn.flush_operator_states().unwrap();
		assert_eq!(op.load_distinct_state(&mut txn).unwrap().entries.len(), 0);
	}

	#[test]
	fn tick_keeps_recently_touched_entries() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let op = make_op(3, Some(10_000_000), &engine);
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			engine.catalog(),
			Interceptors::new(),
			engine.clock().clone(),
		);

		// Insert k=42 at t = 1000ms
		op.apply(&mut txn, build_insert(42, 1)).unwrap();

		// Advance to t = 1015ms, re-insert k=42 (refreshes last_seen_nanos)
		mock_clock.advance_millis(15);
		op.apply(&mut txn, build_insert(42, 99)).unwrap();

		// Insert k=43 at t = 1015ms (this and k=42 are both fresh)
		op.apply(&mut txn, build_insert(43, 2)).unwrap();

		// Tick at t = 1020ms (5ms since both were last touched - within row)
		mock_clock.advance_millis(5);
		op.tick(
			&mut txn,
			Tick {
				now: DateTime::from_nanos(mock_clock.now_nanos()),
			},
		)
		.unwrap();
		txn.flush_operator_states().unwrap();
		assert_eq!(op.load_distinct_state(&mut txn).unwrap().entries.len(), 2);
	}
}
