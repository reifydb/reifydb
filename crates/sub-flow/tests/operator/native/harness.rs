// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	marker::PhantomData,
	mem,
	ops::{Bound, Index},
	sync::Arc,
};

use reifydb_abi::flow::diff::DiffType;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	actors::pending::Pending,
	common::CommitVersion,
	interface::{catalog::flow::FlowNodeId, change::Change},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::{
	config::Config,
	operator::{
		FFIOperatorAdapter, OperatorLogic, OperatorMetadata,
		context::{OperatorContext, StateApi, StoreApi},
	},
	testing::{builders::TestChangeBuilder, harness::FFIOperatorHarness},
};
use reifydb_sub_flow::{
	operator::{
		Operator,
		context::native::NativeOperatorContext,
		native::{FlowNativeBridge, NativeBridgedOperator, NativeOperatorAdapter},
	},
	transaction::{DeferredParams, FlowTransaction, allocators::FlowAllocators},
};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	Result,
	value::{Value, row_number::RowNumber},
};
use serde::de::DeserializeOwned;

pub struct NativeOperatorHarness<C: OperatorLogic + OperatorMetadata + 'static> {
	engine: TestEngine,
	operator: NativeBridgedOperator,
	node_id: FlowNodeId,
	version: u64,
	pending: Pending,
	allocators: FlowAllocators,
	current: Option<FlowTransaction>,
	history: Vec<Change>,
	_phantom: PhantomData<C>,
}

impl<C: OperatorLogic + OperatorMetadata + 'static> NativeOperatorHarness<C> {
	pub fn builder() -> NativeOperatorHarnessBuilder<C> {
		NativeOperatorHarnessBuilder::new()
	}

	fn begin_txn(&mut self) -> FlowTransaction {
		let query = self.engine.multi().begin_query().expect("begin_query");
		let state_query = self.engine.multi().begin_query().expect("begin_query");
		FlowTransaction::deferred_from_parts(DeferredParams {
			version: CommitVersion(self.version),
			pending: mem::take(&mut self.pending),
			base_pending: Arc::new(Pending::new()),
			query,
			state_query,
			single: self.engine.inner().single().clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(1000)),
			allocators: self.allocators.clone(),
		})
	}

	fn end_txn(&mut self, mut txn: FlowTransaction) {
		self.pending = txn.take_pending();
		self.version += 1;
	}

	pub fn apply(&mut self, input: Change) -> Result<Change> {
		let mut txn = self.begin_txn();
		let output = self.operator.apply(&mut txn, input)?;
		txn.flush_operator_states()?;
		self.end_txn(txn);
		self.history.push(output.clone());
		Ok(output)
	}

	pub fn apply_without_flush(&mut self, input: Change) -> Result<Change> {
		let mut txn = self.begin_txn();
		let output = self.operator.apply(&mut txn, input)?;
		self.current = Some(txn);
		self.history.push(output.clone());
		Ok(output)
	}

	pub fn flush(&mut self) -> Result<()> {
		let mut txn = match self.current.take() {
			Some(txn) => txn,
			None => self.begin_txn(),
		};
		txn.flush_operator_states()?;
		self.end_txn(txn);
		Ok(())
	}

	pub fn state_value<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Option<V> {
		let node = self.node_id;
		if let Some(txn) = self.current.as_mut() {
			let mut bridge = FlowNativeBridge::new(txn, node);
			let mut ctx = NativeOperatorContext::new(&mut bridge, node);
			return ctx.state().get::<V>(key).expect("state get");
		}
		let mut txn = self.begin_txn();
		let value = {
			let mut bridge = FlowNativeBridge::new(&mut txn, node);
			let mut ctx = NativeOperatorContext::new(&mut bridge, node);
			ctx.state().get::<V>(key).expect("state get")
		};
		self.end_txn(txn);
		value
	}

	pub fn seed_store(&mut self, rows: &[(EncodedKey, EncodedRow)]) {
		let keys: Vec<EncodedKey> = rows.iter().map(|(k, _)| k.clone()).collect();
		let values: Vec<EncodedRow> = rows.iter().map(|(_, v)| v.clone()).collect();
		let mut txn = self.begin_txn();
		txn.set_batch(&keys, &values).expect("seed_store set_batch");
		self.end_txn(txn);
	}

	pub fn store_range(
		&mut self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Vec<(EncodedKey, EncodedRow)> {
		let node = self.node_id;
		let mut txn = self.begin_txn();
		let rows = {
			let mut bridge = FlowNativeBridge::new(&mut txn, node);
			let mut ctx = NativeOperatorContext::new(&mut bridge, node);
			ctx.store().range(start, end).expect("store range")
		};
		self.end_txn(txn);
		rows
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

	pub fn node_id(&self) -> FlowNodeId {
		self.node_id
	}
}

impl<C: OperatorLogic + OperatorMetadata + 'static> Index<usize> for NativeOperatorHarness<C> {
	type Output = Change;

	fn index(&self, index: usize) -> &Self::Output {
		&self.history[index]
	}
}

pub struct NativeOperatorHarnessBuilder<C> {
	config: HashMap<String, Value>,
	node_id: FlowNodeId,
	version: CommitVersion,
	_phantom: PhantomData<C>,
}

impl<C: OperatorLogic + OperatorMetadata + 'static> Default for NativeOperatorHarnessBuilder<C> {
	fn default() -> Self {
		Self::new()
	}
}

impl<C: OperatorLogic + OperatorMetadata + 'static> NativeOperatorHarnessBuilder<C> {
	pub fn new() -> Self {
		Self {
			config: HashMap::new(),
			node_id: FlowNodeId(1),
			version: CommitVersion(1),
			_phantom: PhantomData,
		}
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

	pub fn with_node_id(mut self, node_id: FlowNodeId) -> Self {
		self.node_id = node_id;
		self
	}

	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	pub fn build(self) -> Result<NativeOperatorHarness<C>> {
		let engine = TestEngine::new();
		let core = C::create(
			self.node_id,
			&Config::new(<C as OperatorMetadata>::NAME, self.config.clone().into_iter().collect()),
		)?;
		let capabilities = <C as OperatorMetadata>::CAPABILITIES;
		let adapter = NativeOperatorAdapter::new(core, self.node_id, capabilities);
		let operator = NativeBridgedOperator::new(Box::new(adapter), self.node_id, capabilities);

		Ok(NativeOperatorHarness {
			engine,
			operator,
			node_id: self.node_id,
			version: self.version.0,
			pending: Pending::new(),
			allocators: FlowAllocators::new(),
			current: None,
			history: Vec::new(),
			_phantom: PhantomData,
		})
	}
}

#[derive(Debug, PartialEq)]
struct ColumnsRender {
	names: Vec<String>,
	row_numbers: Vec<RowNumber>,
	rows: Vec<Vec<Value>>,
}

#[derive(Debug, PartialEq)]
struct DiffRender {
	kind: DiffType,
	pre: Option<ColumnsRender>,
	post: Option<ColumnsRender>,
}

fn render_columns(cols: &Columns) -> ColumnsRender {
	ColumnsRender {
		names: (0..cols.len()).map(|i| cols.name_at(i).text().to_string()).collect(),
		row_numbers: cols.row_numbers.iter().copied().collect(),
		rows: (0..cols.row_count()).map(|r| cols.row(r)).collect(),
	}
}

fn render_change(change: &Change) -> Vec<DiffRender> {
	change.diffs
		.iter()
		.map(|d| DiffRender {
			kind: d.kind(),
			pre: d.pre().map(render_columns),
			post: d.post().map(render_columns),
		})
		.collect()
}

fn run_ffi<C>(config: &[(&str, Value)], inputs: &[Change]) -> Vec<Change>
where
	C: OperatorLogic + OperatorMetadata + 'static,
{
	let mut harness = FFIOperatorHarness::<FFIOperatorAdapter<C>>::builder()
		.with_config(config.iter().cloned())
		.build()
		.expect("ffi harness build");
	inputs.iter().map(|input| harness.apply(input.clone()).expect("ffi apply")).collect()
}

fn run_native<C>(config: &[(&str, Value)], inputs: &[Change]) -> Vec<Change>
where
	C: OperatorLogic + OperatorMetadata + 'static,
{
	let mut harness = NativeOperatorHarness::<C>::builder()
		.with_config(config.iter().cloned())
		.build()
		.expect("native harness build");
	inputs.iter().map(|input| harness.apply(input.clone()).expect("native apply")).collect()
}

pub fn assert_backend_parity<C>(config: Vec<(&str, Value)>, scenarios: &[(&str, Vec<Change>)])
where
	C: OperatorLogic + OperatorMetadata + 'static,
{
	for (name, inputs) in scenarios {
		let ffi = run_ffi::<C>(&config, inputs);
		let native = run_native::<C>(&config, inputs);

		assert_eq!(
			ffi.len(),
			native.len(),
			"scenario '{name}': ffi emitted {} outputs, native emitted {}",
			ffi.len(),
			native.len()
		);

		for (i, (f, n)) in ffi.iter().zip(native.iter()).enumerate() {
			assert_eq!(
				render_change(f),
				render_change(n),
				"scenario '{name}' apply #{i}: ffi vs native emitted-output mismatch"
			);
		}
	}
}
