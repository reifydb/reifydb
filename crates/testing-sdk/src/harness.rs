// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	ffi::c_void,
	marker::PhantomData,
	ops::Index,
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		bytes::EncodedBytes,
		operator::state::OperatorState,
		shape::{RowFamily, RowShape},
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin},
	},
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
	},
	row::Row,
	state::timer::TimerKind,
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		change::BorrowedChange,
		extern_c::{
			binding::{
				arena::Arena,
				context::ExternCContext,
				operator::ExternCOperator,
				wrapper::{OperatorWrapper, extern_c_apply},
			},
			wire::context::ExternCContextRaw,
		},
		timer::Timer,
	},
};
use reifydb_testing_chaos::operator::subject::Subject;
use reifydb_value::{
	Result as ValueResult,
	config::Config,
	count::Count,
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, value_type::ValueType},
};

use crate::{
	builders::TestChangeBuilder,
	callbacks::create_test_callbacks,
	context::{ArmedTimer, TestContext},
	registry::{TestBuilderRegistry, into_diffs, with_registry},
	state::TestStateStore,
};

type DictionarySeed = (String, u64, ValueType, Vec<(u128, Value)>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReclaimedGroups {
	pub groups: Count,
	pub keys: Count,
}

pub struct ExternCOperatorHarness<T: ExternCOperator> {
	operator: T,
	context: Box<TestContext>,
	extern_c_context: Box<ExternCContextRaw>,
	config: HashMap<String, Value>,
	operator_id: OperatorId,
	clock: Clock,
	history: Vec<Change>,

	builder_registry: TestBuilderRegistry,

	input_arena: Arena,
}

impl<T: ExternCOperator> ExternCOperatorHarness<T> {
	pub fn builder() -> ExternCOperatorHarnessBuilder<T> {
		ExternCOperatorHarnessBuilder::new()
	}

	fn refresh_written_at(&mut self) {
		self.extern_c_context.written_at_nanos = self.clock.now().to_nanos();
	}

	pub fn apply(&mut self, input: Change) -> Result<Change> {
		self.refresh_written_at();
		let version = input.version;
		let changed_at = input.changed_at;
		let origin = input.origin.clone();

		self.input_arena.clear();
		let extern_c_change = self.input_arena.marshal_change(&input);
		let extern_c_ctx_ptr = &mut *self.extern_c_context as *mut ExternCContextRaw;

		let result: Result<()> = with_registry(&self.builder_registry, || {
			let mut op_ctx = ExternCContext::new(extern_c_ctx_ptr);
			// SAFETY: extern_c_change lives on this stack frame and the arena backing its
			// buffers is neither cleared nor dropped until after the closure returns, so
			// the borrow cannot outlive its data.
			let borrowed = unsafe { BorrowedChange::from_raw(&extern_c_change as *const _) };
			self.operator.apply(&mut op_ctx, borrowed)
		});

		drop(input);
		result?;

		let emitted = self.builder_registry.drain_diffs();
		let diffs = into_diffs(emitted);
		let output = match origin {
			ChangeOrigin::Flow(operator) => Change::from_flow(operator, version, diffs, changed_at),
			ChangeOrigin::Object(_) => Change::from_flow(self.operator_id, version, diffs, changed_at),
		};
		self.history.push(output.clone());
		Ok(output)
	}

	pub fn fire_timer(&mut self, due: DateTime, kind: TimerKind, key: &[u8]) -> Result<()> {
		self.refresh_written_at();
		let extern_c_ctx_ptr = &mut *self.extern_c_context as *mut ExternCContextRaw;
		with_registry(&self.builder_registry, || {
			let mut op_ctx = ExternCContext::new(extern_c_ctx_ptr);
			self.operator.on_timer(
				&mut op_ctx,
				Timer {
					due,
					kind,
					key,
				},
			)
		})
	}

	pub fn on_timer(&mut self, at: DateTime, kind: TimerKind, key: &[u8]) -> Result<Option<Change>> {
		let version = self.version();
		self.fire_timer(at, kind, key)?;
		let diffs = into_diffs(self.builder_registry.drain_diffs());
		if diffs.is_empty() {
			return Ok(None);
		}
		let output = Change::from_flow(self.operator_id, version, diffs, at);
		self.history.push(output.clone());
		Ok(Some(output))
	}

	pub fn armed_timers(&self) -> Vec<ArmedTimer> {
		self.context.armed_timers()
	}

	pub fn advance_watermark(&mut self, at: DateTime) -> Result<usize> {
		const MAX_ROUNDS: usize = 8192;

		self.context.set_flow_watermark(at);
		let mut fired = 0usize;
		for _ in 0..MAX_ROUNDS {
			let due = self.context.take_due_timers(at);
			if due.is_empty() {
				return Ok(fired);
			}
			for timer in due {
				self.fire_timer(timer.due, timer.kind, &timer.key)?;
				fired += 1;
			}
		}
		panic!("advance_watermark kept finding due timers after {MAX_ROUNDS} rounds; an operator is \
			 re-arming at or below the watermark it was just woken for, which would spin the real \
			 wheel too")
	}

	pub fn group_id(&self, group_key: &[u8]) -> Option<GroupId> {
		let dictionary_key = OperatorStateKey::encoded(
			self.operator_id,
			GroupId::ROOT,
			Keyspace::GROUP_DICTIONARY,
			group_key,
		);
		self.context
			.get_state(&dictionary_key)
			.filter(|bytes| bytes.len() >= 8)
			.map(|bytes| GroupId(u64::from_le_bytes(bytes[..8].try_into().unwrap())))
	}

	pub fn reclaim_groups(&mut self, groups: &[GroupId]) -> ReclaimedGroups {
		let removed = self.erase_group_state(groups, |_| true);
		Self::reclaimed(groups, removed)
	}

	pub fn reclaim_group_data(&mut self, groups: &[GroupId]) -> ReclaimedGroups {
		let removed = self.erase_group_state(groups, |keyspace| keyspace.is_data());
		Self::reclaimed(groups, removed)
	}

	pub fn reclaim_group_identity(&mut self, groups: &[GroupId]) -> ReclaimedGroups {
		let removed = self.erase_group_state(groups, |keyspace| keyspace.is_identity());
		Self::reclaimed(groups, removed)
	}

	fn reclaimed(groups: &[GroupId], removed: usize) -> ReclaimedGroups {
		ReclaimedGroups {
			groups: Count::new(groups.len() as u64),
			keys: Count::new(removed as u64),
		}
	}

	fn erase_group_state(&mut self, groups: &[GroupId], erase: impl Fn(Keyspace) -> bool) -> usize {
		let mut state = self.context.state_store().lock();
		let before = state.len();
		state.retain(|key, _| {
			let Some(decoded) = OperatorStateKey::decode(key) else {
				return true;
			};
			if decoded.operator != self.operator_id {
				return true;
			}
			decoded.group.is_root() || !groups.contains(&decoded.group) || !erase(decoded.keyspace)
		});
		before - state.len()
	}

	pub fn state_value<V: OperatorState>(&mut self, key: &GroupStateKey) -> Option<V> {
		let mut ctx = self.create_operator_context();
		ctx.state().get::<V>(key).expect("state get")
	}

	pub fn insert(&mut self, row: Row) -> &mut Self {
		let change = TestChangeBuilder::new().insert(row).build();
		self.apply(change).expect("insert failed");
		self
	}

	pub fn update(&mut self, pre: Row, post: Row) -> &mut Self {
		let change = TestChangeBuilder::new().update(pre, post).build();
		self.apply(change).expect("update failed");
		self
	}

	pub fn remove(&mut self, row: Row) -> &mut Self {
		let change = TestChangeBuilder::new().remove(row).build();
		self.apply(change).expect("remove failed");
		self
	}

	pub fn history_len(&self) -> usize {
		self.history.len()
	}

	pub fn last_change(&self) -> Option<&Change> {
		self.history.last()
	}

	pub fn clear_history(&mut self) {
		self.history.clear();
	}

	pub fn version(&self) -> CommitVersion {
		(*self.context).version()
	}

	pub fn set_version(&mut self, version: CommitVersion) {
		(*self.context).set_version(version);
	}

	pub fn state(&self) -> TestStateStore {
		let store = self.context.state_store();
		let data = store.lock();
		let mut result = TestStateStore::new();
		for (k, v) in data.iter() {
			result.set(k.clone(), v.clone());
		}
		result
	}

	pub fn assert_state<K>(&self, key: K, expected: Value)
	where
		K: EncodableKey,
	{
		let encoded_key = key.encode();
		let store = self.state();
		let shape = RowShape::testing(RowFamily::Pod, &[expected.get_type()]);

		store.assert_value(&encoded_key, &[expected], &shape);
	}

	pub fn logs(&self) -> Vec<String> {
		(*self.context).logs()
	}

	pub fn clear_logs(&self) {
		(*self.context).clear_logs()
	}

	pub fn snapshot_state(&self) -> HashMap<EncodedKey, EncodedBytes> {
		self.state().snapshot()
	}

	pub fn restore_state(&mut self, snapshot: HashMap<EncodedKey, EncodedBytes>) {
		(*self.context).clear_state();
		for (k, v) in snapshot {
			(*self.context).set_state(k, v.0.to_vec());
		}
	}

	pub fn reset(&mut self) -> Result<()> {
		(*self.context).clear_state();
		(*self.context).clear_logs();
		(*self.context).set_version(CommitVersion(1));
		self.history.clear();

		self.operator =
			T::new(self.operator_id, &Config::new("operator", self.config.clone().into_iter().collect()))?;
		Ok(())
	}

	pub fn create_operator_context(&mut self) -> ExternCContext {
		self.refresh_written_at();
		ExternCContext::new(&mut *self.extern_c_context as *mut ExternCContextRaw)
	}

	pub fn operator(&self) -> &T {
		&self.operator
	}

	pub fn operator_mut(&mut self) -> &mut T {
		&mut self.operator
	}

	pub fn operator_id(&self) -> OperatorId {
		self.operator_id
	}
}

impl<T: ExternCOperator> Index<usize> for ExternCOperatorHarness<T> {
	type Output = Change;

	fn index(&self, index: usize) -> &Self::Output {
		&self.history[index]
	}
}

pub struct ExternCOperatorHarnessBuilder<T: ExternCOperator> {
	config: HashMap<String, Value>,
	operator_id: OperatorId,
	version: CommitVersion,
	clock: Clock,
	initial_state: HashMap<EncodedKey, EncodedBytes>,
	dictionaries: Vec<DictionarySeed>,
	_phantom: PhantomData<T>,
}

impl<T: ExternCOperator> Default for ExternCOperatorHarnessBuilder<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: ExternCOperator> ExternCOperatorHarnessBuilder<T> {
	pub fn new() -> Self {
		Self {
			config: HashMap::new(),
			operator_id: OperatorId(1),
			version: CommitVersion(1),
			clock: Clock::Mock(MockClock::new(0)),
			initial_state: HashMap::new(),
			dictionaries: Vec::new(),
			_phantom: PhantomData,
		}
	}

	pub fn with_clock(mut self, clock: Clock) -> Self {
		self.clock = clock;
		self
	}

	pub fn with_config<I, K>(mut self, config: I) -> Self
	where
		I: IntoIterator<Item = (K, Value)>,
		K: Into<String>,
	{
		self.config = config.into_iter().map(|(k, v)| (k.into(), v)).collect();
		self
	}

	pub fn add_config(mut self, key: impl Into<String>, value: Value) -> Self {
		self.config.insert(key.into(), value);
		self
	}

	pub fn with_node_id(mut self, operator_id: OperatorId) -> Self {
		self.operator_id = operator_id;
		self
	}

	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	pub fn with_initial_state<K>(mut self, key: K, value: Vec<u8>) -> Self
	where
		K: EncodableKey,
	{
		self.initial_state.insert(key.encode(), EncodedBytes(CowVec::new(value)));
		self
	}

	pub fn with_dictionary(
		mut self,
		name: impl Into<String>,
		id: u64,
		id_type: ValueType,
		entries: Vec<(u128, Value)>,
	) -> Self {
		self.dictionaries.push((name.into(), id, id_type, entries));
		self
	}

	pub fn build(self) -> Result<ExternCOperatorHarness<T>> {
		let context = Box::new(TestContext::new(self.version));

		for (k, v) in self.initial_state {
			context.set_state(k, v.0.to_vec());
		}

		for (name, id, id_type, entries) in &self.dictionaries {
			context.seed_dictionary_interning(name, *id, id_type.clone(), entries);
		}

		let extern_c_context = Box::new(ExternCContextRaw {
			txn_ptr: &*context as *const TestContext as *mut c_void,
			written_at_nanos: self.clock.now().to_nanos(),
			operator_id: self.operator_id.0,
			callbacks: create_test_callbacks(),
		});

		let operator =
			T::new(self.operator_id, &Config::new("operator", self.config.clone().into_iter().collect()))?;

		Ok(ExternCOperatorHarness {
			operator,
			context,
			extern_c_context,
			config: self.config,
			operator_id: self.operator_id,
			clock: self.clock,
			history: Vec::new(),
			builder_registry: TestBuilderRegistry::new(),
			input_arena: Arena::new(),
		})
	}
}

pub fn drive_extern_c_apply<O: ExternCOperator + OperatorMetadata>(input: &Change) -> i32 {
	let context = Box::new(TestContext::new(CommitVersion(1)));
	let mut extern_c_context = ExternCContextRaw {
		txn_ptr: &*context as *const TestContext as *mut c_void,
		written_at_nanos: 0,
		operator_id: 1,
		callbacks: create_test_callbacks(),
	};

	let operator = O::new(OperatorId(1), &Config::new("operator", BTreeMap::new())).expect("create operator");
	let mut wrapper = OperatorWrapper::new(operator);

	let mut arena = Arena::new();
	let extern_c_change = arena.marshal_change(input);

	let registry = TestBuilderRegistry::new();
	// SAFETY: the wrapper, context and change all outlive the call, and the arena backing the
	// change's buffers is still alive, so every pointer crossing the boundary is valid for its
	// whole duration.
	with_registry(&registry, || unsafe {
		extern_c_apply::<O>(
			wrapper.as_ptr(),
			&mut extern_c_context as *mut ExternCContextRaw,
			&extern_c_change as *const _,
		)
	})
}

pub struct TestMetadataHarness;

impl TestMetadataHarness {
	pub fn assert_name<T: OperatorMetadata>(expected: &str) {
		assert_eq!(T::NAME, expected, "Operator name mismatch. Expected: {}, Actual: {}", expected, T::NAME);
	}

	pub fn assert_version<T: OperatorMetadata>(expected: &str) {
		assert_eq!(
			T::VERSION,
			expected,
			"Operator version mismatch. Expected: {}, Actual: {}",
			expected,
			T::VERSION
		);
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::tag::ValueKind;
	use reifydb_core::{
		common::CommitVersion,
		interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	};
	use reifydb_sdk::{
		common::extern_c::{
			binding::builder::{ColumnsBuilder, CommittedColumn},
			wire::callbacks::builder::EmitDiffKind,
		},
		flow::operator::{
			OperatorMetadata,
			change::{BorrowedChange, BorrowedColumns},
			column::operator::OperatorColumn,
			context::GuestContext,
			extern_c::binding::{context::ExternCContext, operator::ExternCOperator},
		},
		row,
	};
	use reifydb_value::value::{diff_type::DiffType, row_number::RowNumber};

	use super::{
		super::helpers::{encode_key, probe_row_key},
		*,
	};
	use crate::builders::{TestChangeBuilder, TestOperatorRowBuilder};

	struct TestOperator {
		_node_id: OperatorId,
		_config: Config,
	}

	impl OperatorMetadata for TestOperator {
		const NAME: &'static str = "test_operator";
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Simple pass-through test operator";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl ExternCOperator for TestOperator {
		fn new(operator_id: OperatorId, config: &Config) -> Result<Self> {
			Ok(Self {
				_node_id: operator_id,
				_config: config.clone(),
			})
		}

		fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
			forward_diffs_passthrough(ctx, &input)
		}
	}

	struct StatefulTestOperator;

	impl OperatorMetadata for StatefulTestOperator {
		const NAME: &'static str = "stateful_test_operator";
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Stateful test operator that stores values";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl ExternCOperator for StatefulTestOperator {
		fn new(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
			for diff in input.diffs() {
				let post = match diff.kind() {
					DiffType::Insert | DiffType::Update => Some(diff.post()),
					DiffType::Remove => None,
				};
				if let Some(columns) = post {
					let row_numbers = columns.row_numbers();
					let first_int8 = columns
						.columns()
						.next()
						// SAFETY: the fixtures that drive this operator only ever
						// build a leading Int8 column, so the requested element
						// type matches the buffer's.
						.and_then(|c| unsafe { c.as_slice::<i64>() })
						.and_then(|s| s.first().copied());
					if let (Some(&rn), Some(v)) = (row_numbers.first(), first_int8) {
						ctx.state().set::<i64>(&probe_row_key(rn), &v)?;
					}
				}
			}
			forward_diffs_passthrough(ctx, &input)
		}
	}

	fn forward_diffs_passthrough(ctx: &mut ExternCContext, input: &BorrowedChange<'_>) -> Result<()> {
		let mut builder = ctx.builder();
		for diff in input.diffs() {
			match diff.kind() {
				DiffType::Insert => {
					let (cols, names) = clone_columns(&mut builder, diff.post())?;
					let post: Vec<CommittedColumn> = cols;
					let post_names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
					let row_numbers: Vec<RowNumber> =
						diff.post().row_numbers().iter().copied().map(RowNumber).collect();
					let _ = post;
					builder.emit_insert(&post, &post_names, &row_numbers)?;
				}
				DiffType::Update => {
					let (pre_cols, pre_names) = clone_columns(&mut builder, diff.pre())?;
					let (post_cols, post_names) = clone_columns(&mut builder, diff.post())?;
					let pre_names: Vec<&str> = pre_names.iter().map(|s| s.as_str()).collect();
					let post_names: Vec<&str> = post_names.iter().map(|s| s.as_str()).collect();
					let pre_row_count = diff.pre().row_count();
					let post_row_count = diff.post().row_count();
					let pre_row_numbers: Vec<RowNumber> =
						diff.pre().row_numbers().iter().copied().map(RowNumber).collect();
					let post_row_numbers: Vec<RowNumber> =
						diff.post().row_numbers().iter().copied().map(RowNumber).collect();
					builder.emit_update(
						&pre_cols,
						&pre_names,
						pre_row_count,
						&pre_row_numbers,
						&post_cols,
						&post_names,
						post_row_count,
						&post_row_numbers,
					)?;
				}
				DiffType::Remove => {
					let (cols, names) = clone_columns(&mut builder, diff.pre())?;
					let names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
					let row_numbers: Vec<RowNumber> =
						diff.pre().row_numbers().iter().copied().map(RowNumber).collect();
					builder.emit_remove(&cols, &names, &row_numbers)?;
				}
			}
		}
		// Suppress emit-kind-not-used warning by silencing the import.
		let _ = EmitDiffKind::Insert;
		Ok(())
	}

	fn clone_columns(
		builder: &mut ColumnsBuilder<'_>,
		cols: BorrowedColumns<'_>,
	) -> Result<(Vec<CommittedColumn>, Vec<String>)> {
		let row_count = cols.row_count();
		let mut committed: Vec<CommittedColumn> = Vec::new();
		let mut names: Vec<String> = Vec::new();
		for col in cols.columns() {
			let type_code = col.type_code();
			let bytes = col.data_bytes();
			let active = builder.acquire(type_code, row_count.max(1))?;
			active.grow(bytes.len().max(row_count))?;
			let dst = active.data_ptr();
			if !dst.is_null() && !bytes.is_empty() {
				// SAFETY: dst is non-null and the preceding grow() sized it to at
				// least bytes.len(); source and destination are distinct
				// allocations.
				unsafe {
					core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
				}
			}
			if matches!(type_code, ValueKind::Utf8 | ValueKind::Blob) {
				let off = col.offsets();
				let dst_off = active.offsets_ptr();
				if !dst_off.is_null() && !off.is_empty() {
					// SAFETY: dst_off is non-null and the builder sizes the
					// offsets region from the same row count off was read at;
					// the buffers do not alias.
					unsafe {
						core::ptr::copy_nonoverlapping(off.as_ptr(), dst_off, off.len());
					}
				}
			}
			let c = active.commit(row_count)?;
			committed.push(c);
			names.push(col.name().to_string());
		}
		Ok((committed, names))
	}

	#[test]
	fn test_operator_metadata() {
		TestMetadataHarness::assert_name::<TestOperator>("test_operator");
		TestMetadataHarness::assert_version::<TestOperator>("1.0.0");
	}

	#[test]
	fn test_harness_builder() {
		let result = ExternCOperatorHarnessBuilder::<TestOperator>::new()
			.with_node_id(OperatorId(42))
			.with_version(CommitVersion(10))
			.add_config("key", Value::Utf8("value".into()))
			.build();

		assert!(result.is_ok());

		let harness = result.unwrap();
		assert_eq!(harness.operator_id, 42);
		assert_eq!(harness.version(), 10);
	}

	#[test]
	fn test_harness_with_stateful_operator() {
		let mut harness = ExternCOperatorHarnessBuilder::<StatefulTestOperator>::new()
			.with_node_id(OperatorId(1))
			.build()
			.expect("Failed to build harness");

		let input = TestChangeBuilder::new().insert_row(1, vec![Value::Int8(42i64)]).build();

		let output = harness.apply(input).expect("Apply failed");

		assert_eq!(output.diffs.len(), 1);

		// State is wrapped in the canonical operator_state row plus a postcard payload, so
		// assertions have to go through the typed accessor.
		let state = harness.state();
		state.assert_typed_value::<i64>(probe_row_key(1).as_encoded(), &42i64);
	}

	#[test]
	fn test_harness_history_index() {
		let mut harness = ExternCOperatorHarnessBuilder::<StatefulTestOperator>::new()
			.with_node_id(OperatorId(1))
			.build()
			.expect("Failed to build harness");

		assert_eq!(harness.history_len(), 0);
		assert!(harness.last_change().is_none());

		let input_a = TestChangeBuilder::new().insert_row(1, vec![Value::Int8(1i64)]).build();
		harness.apply(input_a).expect("apply a failed");
		assert_eq!(harness.history_len(), 1);

		let input_b = TestChangeBuilder::new().insert_row(2, vec![Value::Int8(2i64)]).build();
		harness.apply(input_b).expect("apply b failed");
		assert_eq!(harness.history_len(), 2);

		assert_eq!(harness[0].diffs.len(), 1);
		assert_eq!(harness[1].diffs.len(), 1);

		harness.insert(TestOperatorRowBuilder::new(3).add_value(Value::Int8(3i64)).build());
		assert_eq!(harness.history_len(), 3);

		assert!(harness.last_change().is_some());

		// clear_history must not disturb operator state.
		let state_count_before = harness.state().len();
		harness.clear_history();
		assert_eq!(harness.history_len(), 0);
		assert!(harness.last_change().is_none());
		assert_eq!(harness.state().len(), state_count_before);
	}

	#[test]
	fn test_harness_multiple_operations() {
		let mut harness = ExternCOperatorHarnessBuilder::<StatefulTestOperator>::new()
			.build()
			.expect("Failed to build harness");

		let input1 = TestChangeBuilder::new()
			.insert_row(1, vec![Value::Int8(10i64)])
			.insert_row(2, vec![Value::Int8(20i64)])
			.build();

		harness.apply(input1).expect("First apply failed");

		let state = harness.state();
		assert_eq!(state.len(), 2);

		let input2 = TestChangeBuilder::new().insert_row(RowNumber(3), vec![Value::Int8(30i64)]).build();

		harness.apply(input2).expect("Second apply failed");

		let state = harness.state();
		state.assert_typed_value::<i64>(probe_row_key(1).as_encoded(), &10i64);
		state.assert_typed_value::<i64>(probe_row_key(2).as_encoded(), &20i64);
		state.assert_typed_value::<i64>(probe_row_key(3).as_encoded(), &30i64);

		assert_eq!(state.len(), 3);
	}

	const MILLI: u64 = 1_000_000;

	const REARM_LIMIT: i64 = 3;

	struct TimerTestOperator;

	impl OperatorMetadata for TimerTestOperator {
		const NAME: &'static str = "timer_test_operator";
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Arms one Seal timer per inserted row and records every fire";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl ExternCOperator for TimerTestOperator {
		fn new(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
			// Row n arms a Seal timer at n milliseconds, so a test picks which timers are due
			// purely by choosing the row numbers it inserts.
			for diff in input.diffs() {
				if !matches!(diff.kind(), DiffType::Insert) {
					continue;
				}
				for &row_number in diff.post().row_numbers() {
					ctx.arm_timer(
						DateTime::from_nanos(row_number * MILLI),
						TimerKind::Seal,
						&encode_key(format!("row_{row_number}")),
					)?;
				}
			}
			Ok(())
		}

		fn on_timer(&mut self, ctx: &mut ExternCContext, timer: Timer<'_>) -> Result<()> {
			// State is what proves on_timer reached the operator; a fired count alone would
			// still pass if the harness popped the wheel and dropped the callback.
			ctx.state().set::<i64>(&probe_row_key(timer.due.to_nanos()), &1i64)
		}
	}

	struct SealEmittingOperator;

	#[derive(Clone)]
	struct SealRow {
		total: i64,
	}

	row!(SealRow {
		total: i64
	});

	impl OperatorMetadata for SealEmittingOperator {
		const NAME: &'static str = "seal_emitting_operator";
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str =
			"Emits a finalized row from on_timer, the way a windowed operator seals";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl ExternCOperator for SealEmittingOperator {
		fn new(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, _ctx: &mut ExternCContext, _input: BorrowedChange<'_>) -> Result<()> {
			Ok(())
		}

		fn on_timer(&mut self, ctx: &mut ExternCContext, timer: Timer<'_>) -> Result<()> {
			ctx.emit_insert(
				&[SealRow {
					total: timer.due.to_millis() as i64,
				}],
				&[RowNumber(1)],
			)
		}
	}

	#[test]
	fn a_timer_emission_reaches_the_caller_as_a_change() {
		// Without a drain per timer, emitted diffs sit in the registry and surface attributed to
		// whichever apply comes next, or vanish when none does.
		let mut harness = ExternCOperatorHarness::<SealEmittingOperator>::builder().build().unwrap();

		let change = harness
			.on_timer(DateTime::from_epoch_millis(7_000).unwrap(), TimerKind::Seal, &[])
			.expect("firing a seal must succeed")
			.expect("an operator that emits from on_timer must hand its change back");

		assert_eq!(change.diffs.len(), 1, "the seal emitted exactly one diff, so exactly one must arrive");
		assert_eq!(
			change.changed_at,
			DateTime::from_epoch_millis(7_000).unwrap(),
			"a timer's change carries the instant it fired at, not the last input's timestamp; \
			 stamping it from an input change is what the host wrapper deliberately avoids"
		);
	}

	#[test]
	fn a_timer_that_emits_nothing_reports_no_change() {
		// The empty case must be None rather than an empty Change, or a driver's drain loop could
		// never tell "nothing left to withdraw" from "withdrew an empty batch" and would spin.
		let mut harness = ExternCOperatorHarness::<TimerTestOperator>::builder().build().unwrap();

		let change = harness
			.on_timer(DateTime::from_epoch_millis(1).unwrap(), TimerKind::Seal, &[])
			.expect("firing a seal must succeed");

		assert!(change.is_none(), "an operator that only touches state must not manufacture a change");
	}

	struct RearmingTimerTestOperator {
		fires: i64,
	}

	impl OperatorMetadata for RearmingTimerTestOperator {
		const NAME: &'static str = "rearming_timer_test_operator";
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Re-arms itself one millisecond later, up to a bounded limit";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl ExternCOperator for RearmingTimerTestOperator {
		fn new(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
			Ok(Self {
				fires: 0,
			})
		}

		fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
			for diff in input.diffs() {
				if !matches!(diff.kind(), DiffType::Insert) {
					continue;
				}
				for &row_number in diff.post().row_numbers() {
					ctx.arm_timer(
						DateTime::from_nanos(row_number * MILLI),
						TimerKind::Seal,
						&encode_key("rearm"),
					)?;
				}
			}
			Ok(())
		}

		fn on_timer(&mut self, ctx: &mut ExternCContext, timer: Timer<'_>) -> Result<()> {
			// Re-arms one millisecond out, still at or below the watermark the test advances
			// to; the limit is what keeps this off advance_watermark's runaway panic.
			self.fires += 1;
			ctx.state().set::<i64>(&probe_row_key(timer.due.to_nanos()), &self.fires)?;
			if self.fires < REARM_LIMIT {
				ctx.arm_timer(
					DateTime::from_nanos(timer.due.to_nanos() + MILLI),
					TimerKind::Seal,
					&encode_key("rearm"),
				)?;
			}
			Ok(())
		}
	}

	#[test]
	fn an_armed_guest_timer_fires_when_the_harness_watermark_passes_it() {
		// The timer must fire because the watermark reached it, with no further input arriving;
		// otherwise a guest test cannot tell "sealed by the clock" from "sealed by the next row".
		let mut harness = ExternCOperatorHarnessBuilder::<TimerTestOperator>::new()
			.with_node_id(OperatorId(1))
			.build()
			.unwrap();

		harness.apply(TestChangeBuilder::new()
			.insert_row(1, vec![Value::Int8(1i64)])
			.insert_row(3, vec![Value::Int8(3i64)])
			.build())
			.unwrap();
		assert_eq!(harness.armed_timers().len(), 2, "each inserted row arms one timer");

		let fired = harness.advance_watermark(DateTime::from_nanos(2 * MILLI)).unwrap();

		assert_eq!(fired, 1, "only the timer at or below the watermark fires");
		let still_armed = harness.armed_timers();
		assert_eq!(still_armed.len(), 1, "the 3ms timer is untouched");
		assert_eq!(still_armed[0].due, DateTime::from_nanos(3 * MILLI));

		let state = harness.state();
		state.assert_typed_value::<i64>(probe_row_key(MILLI).as_encoded(), &1i64);
		assert_eq!(state.len(), 1, "the 3ms timer must not have reached the operator");
	}

	#[test]
	fn advancing_the_harness_watermark_twice_fires_a_timer_once() {
		// A resurrected timer would make every seal fire once per subsequent advance, silently
		// inflating retraction counts.
		let mut harness = ExternCOperatorHarnessBuilder::<TimerTestOperator>::new()
			.with_node_id(OperatorId(1))
			.build()
			.unwrap();

		harness.apply(TestChangeBuilder::new().insert_row(1, vec![Value::Int8(1i64)]).build()).unwrap();

		assert_eq!(harness.advance_watermark(DateTime::from_nanos(2 * MILLI)).unwrap(), 1);
		assert_eq!(harness.advance_watermark(DateTime::from_nanos(2 * MILLI)).unwrap(), 0, "same watermark");
		assert_eq!(harness.advance_watermark(DateTime::from_nanos(9 * MILLI)).unwrap(), 0, "higher watermark");

		assert!(harness.armed_timers().is_empty());
		assert_eq!(harness.state().len(), 1, "exactly one fire reached the operator");
	}

	#[test]
	fn a_timer_rearmed_inside_on_timer_below_the_watermark_fires_in_the_same_advance() {
		// Session windows re-arm on every extending event, so arming below an already-passed
		// watermark is normal and the real wheel picks it up in the same round.
		let mut harness = ExternCOperatorHarnessBuilder::<RearmingTimerTestOperator>::new()
			.with_node_id(OperatorId(1))
			.build()
			.unwrap();

		harness.apply(TestChangeBuilder::new().insert_row(1, vec![Value::Int8(1i64)]).build()).unwrap();

		let fired = harness.advance_watermark(DateTime::from_nanos(9 * MILLI)).unwrap();

		assert_eq!(fired, REARM_LIMIT as usize, "each re-arm below the watermark fires in the same call");
		assert!(harness.armed_timers().is_empty(), "the operator stopped re-arming at the limit");

		let state = harness.state();
		state.assert_typed_value::<i64>(probe_row_key(MILLI).as_encoded(), &1i64);
		state.assert_typed_value::<i64>(probe_row_key(2 * MILLI).as_encoded(), &2i64);
		state.assert_typed_value::<i64>(probe_row_key(3 * MILLI).as_encoded(), &3i64);
	}

	fn group_state_key(operator: OperatorId, group: GroupId, keyspace: Keyspace) -> OperatorStateKey {
		// Must compose the key the way the substrate does, or seeded state is unaddressable by the sweep's
		// phase ranges.
		OperatorStateKey::new(operator, group, keyspace, b"k".to_vec())
	}

	#[test]
	fn the_data_phase_leaves_behind_the_mapping_that_names_the_published_row() {
		// The engine erases a group's data half long before its identity half, and an operator
		// woken in that window can only answer Update because the mapping is still there. Wiping
		// both at once would let an operator that answers Insert pass.
		const NODE: OperatorId = OperatorId(1);
		const GROUP: GroupId = GroupId(7);
		let accumulator = group_state_key(NODE, GROUP, Keyspace::ACCUMULATOR).encode();
		let mapping = group_state_key(NODE, GROUP, Keyspace::ROW_NUMBER_MAPPING).encode();
		let mut harness = ExternCOperatorHarnessBuilder::<TestOperator>::new()
			.with_node_id(NODE)
			.with_initial_state(group_state_key(NODE, GROUP, Keyspace::ACCUMULATOR), vec![1])
			.with_initial_state(group_state_key(NODE, GROUP, Keyspace::ROW_NUMBER_MAPPING), vec![2])
			.build()
			.unwrap();

		let reclaimed = harness.reclaim_group_data(&[GROUP]);
		assert_eq!(reclaimed.keys, Count::new(1), "the data phase takes the accumulator and only that");
		let state = harness.snapshot_state();
		assert!(!state.contains_key(&accumulator), "the accumulator is what the data phase is for");
		assert!(state.contains_key(&mapping), "the mapping has to outlive it - identity trails data");

		let reclaimed = harness.reclaim_group_identity(&[GROUP]);
		assert_eq!(reclaimed.keys, Count::new(1), "the identity phase then takes the mapping");
		assert!(!harness.snapshot_state().contains_key(&mapping));
	}

	#[test]
	fn erasing_a_group_never_reaches_the_root_scoped_dictionary_that_resolves_it() {
		// The root group holds every group's interning dictionary and id counter; erasing it would mint
		// duplicate ids.
		const NODE: OperatorId = OperatorId(1);
		let dictionary = group_state_key(NODE, GroupId::ROOT, Keyspace::GROUP_DICTIONARY).encode();
		let counter = group_state_key(NODE, GroupId::ROOT, Keyspace::NODE_COUNTER).encode();
		let mut harness = ExternCOperatorHarnessBuilder::<TestOperator>::new()
			.with_node_id(NODE)
			.with_initial_state(group_state_key(NODE, GroupId::ROOT, Keyspace::GROUP_DICTIONARY), vec![1])
			.with_initial_state(group_state_key(NODE, GroupId::ROOT, Keyspace::NODE_COUNTER), vec![2])
			.build()
			.unwrap();

		harness.reclaim_groups(&[GroupId::ROOT]);

		let state = harness.snapshot_state();
		assert!(state.contains_key(&dictionary), "the dictionary survives even a sweep naming the root group");
		assert!(state.contains_key(&counter), "so does the id counter");
	}
}

impl<T: ExternCOperator> Subject for ExternCOperatorHarness<T> {
	fn apply(&mut self, change: Change) -> ValueResult<Change> {
		ExternCOperatorHarness::apply(self, change).map_err(Into::into)
	}

	fn tick(&mut self, at_ms: u64) -> ValueResult<Option<Change>> {
		self.on_timer(DateTime::from_epoch_millis(at_ms).unwrap(), TimerKind::Seal, &[]).map_err(Into::into)
	}
}
