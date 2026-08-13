// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, marker::PhantomData, mem, ops::Index};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::operator::OperatorState;
use reifydb_core::{
	actors::pending::{Pending, PendingLayers, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, DiffType},
	},
	key::{Key, kind::KeyKind, operator_state::GroupStateKey},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::{BoxedHostOperator, host::TxnHostContext},
	transaction::{
		ChangeCoordinate, DeferredParams, FlowTransaction,
		deferred::DeferredTransaction,
		substrate::{FlowSubstrate, apply_operator_state},
	},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::flow::operator::{
	GuestOperator, OperatorMetadata,
	context::{GuestContext, GuestState},
	extern_c::binding::operator::ExternCOperatorAdapter,
};
use reifydb_sub_flow::operator::{context::in_process::InProcessContext, mount::mount};
use reifydb_test_harness::engine::TestEngine;
use reifydb_testing_sdk::{builders::TestChangeBuilder, harness::ExternCOperatorHarness};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	Result,
	config::Config,
	value::{Value, datetime::DateTime, row_number::RowNumber},
};

pub struct GuestOperatorHarness<C: GuestOperator + OperatorMetadata + 'static> {
	engine: TestEngine,
	operator: BoxedHostOperator,
	operator_id: OperatorId,
	version: u64,
	pending: Pending,
	substrate: FlowSubstrate,
	current: Option<DeferredTransaction>,
	history: Vec<Change>,
	_phantom: PhantomData<C>,
}

impl<C: GuestOperator + OperatorMetadata + 'static> GuestOperatorHarness<C> {
	pub fn builder() -> GuestOperatorHarnessBuilder<C> {
		GuestOperatorHarnessBuilder::new()
	}

	fn begin_txn(&mut self) -> DeferredTransaction {
		let query = self.engine.multi().begin_query().expect("begin_query");
		let state_query = self.engine.multi().begin_query().expect("begin_query");
		let mut txn = DeferredTransaction::new(DeferredParams {
			version: CommitVersion(self.version),
			pending: PendingLayers::with_top(mem::take(&mut self.pending)),
			query,
			state_query,
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(1000)),
			substrate: self.substrate.clone(),
		});
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(self.version)),
			version: CommitVersion(self.version),
		});
		txn
	}

	fn end_txn(&mut self, mut txn: DeferredTransaction) {
		let pending = txn.take_pending();
		apply_operator_state(&self.substrate.operators, &pending);
		let mut rest = Pending::new();
		for (key, write) in pending.iter_sorted() {
			if matches!(Key::kind(key), Some(KeyKind::OperatorState)) {
				continue;
			}
			match write {
				PendingWrite::Set(row) => rest.insert(key.clone(), row.clone()),
				PendingWrite::Remove {
					announce: true,
				} => rest.remove(key.clone()),
				PendingWrite::Remove {
					announce: false,
				} => rest.remove_silent(key.clone()),
			}
		}
		self.pending = rest;
		self.version += 1;
	}

	pub fn apply(&mut self, input: Change) -> Result<Change> {
		let operator = self.operator_id;
		let mut txn = self.begin_txn();
		let output = {
			let mut host = TxnHostContext::new(&mut txn, operator);
			self.operator.apply(&mut host, input)?
		};
		self.end_txn(txn);
		self.history.push(output.clone());
		Ok(output)
	}


	pub fn state_value<V: OperatorState>(&mut self, key: &GroupStateKey) -> Option<V> {
		let operator = self.operator_id;
		if let Some(txn) = self.current.as_mut() {
			let mut host = TxnHostContext::new(txn, operator);
			let mut ctx = InProcessContext::new(&mut host, operator);
			return ctx.state().get::<V>(key).expect("state get");
		}
		let mut txn = self.begin_txn();
		let value = {
			let mut host = TxnHostContext::new(&mut txn, operator);
			let mut ctx = InProcessContext::new(&mut host, operator);
			ctx.state().get::<V>(key).expect("state get")
		};
		self.end_txn(txn);
		value
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

	pub fn operator_id(&self) -> OperatorId {
		self.operator_id
	}
}

impl<C: GuestOperator + OperatorMetadata + 'static> Index<usize> for GuestOperatorHarness<C> {
	type Output = Change;

	fn index(&self, index: usize) -> &Self::Output {
		&self.history[index]
	}
}

pub struct GuestOperatorHarnessBuilder<C> {
	config: HashMap<String, Value>,
	operator_id: OperatorId,
	version: CommitVersion,
	_phantom: PhantomData<C>,
}

impl<C: GuestOperator + OperatorMetadata + 'static> Default for GuestOperatorHarnessBuilder<C> {
	fn default() -> Self {
		Self::new()
	}
}

impl<C: GuestOperator + OperatorMetadata + 'static> GuestOperatorHarnessBuilder<C> {
	pub fn new() -> Self {
		Self {
			config: HashMap::new(),
			operator_id: OperatorId(1),
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

	pub fn with_node_id(mut self, operator_id: OperatorId) -> Self {
		self.operator_id = operator_id;
		self
	}

	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	pub fn build(self) -> Result<GuestOperatorHarness<C>> {
		let engine = TestEngine::new();
		let core = C::create(
			self.operator_id,
			&Config::new(<C as OperatorMetadata>::NAME, self.config.clone().into_iter().collect()),
		)?;
		let capabilities = <C as OperatorMetadata>::CAPABILITIES;
		let operator = mount(core, self.operator_id, capabilities);

		let substrate = FlowSubstrate {
			operators: engine.inner().operator_state(),
			..FlowSubstrate::default()
		};
		Ok(GuestOperatorHarness {
			engine,
			operator,
			operator_id: self.operator_id,
			version: self.version.0,
			pending: Pending::new(),
			substrate,
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
		row_numbers: cols.row_numbers().to_vec(),
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

fn run_extern_c<C>(config: &[(&str, Value)], inputs: &[Change]) -> Vec<Change>
where
	C: GuestOperator + OperatorMetadata + 'static,
{
	let mut harness = ExternCOperatorHarness::<ExternCOperatorAdapter<C>>::builder()
		.with_config(config.iter().cloned())
		.build()
		.expect("extern-C harness build");
	inputs.iter().map(|input| harness.apply(input.clone()).expect("extern-C apply")).collect()
}

fn run_guest<C>(config: &[(&str, Value)], inputs: &[Change]) -> Vec<Change>
where
	C: GuestOperator + OperatorMetadata + 'static,
{
	let mut harness = GuestOperatorHarness::<C>::builder()
		.with_config(config.iter().cloned())
		.build()
		.expect("host harness build");
	inputs.iter().map(|input| harness.apply(input.clone()).expect("host apply")).collect()
}

pub fn assert_backend_parity<C>(config: Vec<(&str, Value)>, scenarios: &[(&str, Vec<Change>)])
where
	C: GuestOperator + OperatorMetadata + 'static,
{
	for (name, inputs) in scenarios {
		let extern_c = run_extern_c::<C>(&config, inputs);
		let host = run_guest::<C>(&config, inputs);

		assert_eq!(
			extern_c.len(),
			host.len(),
			"scenario '{name}': extern-C emitted {} outputs, host emitted {}",
			extern_c.len(),
			host.len()
		);

		for (i, (f, n)) in extern_c.iter().zip(host.iter()).enumerate() {
			assert_eq!(
				render_change(f),
				render_change(n),
				"scenario '{name}' apply #{i}: extern-C vs host emitted-output mismatch"
			);
		}
	}
}
