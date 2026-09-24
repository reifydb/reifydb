// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, marker::PhantomData, mem, ops::Index};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	delta::RemoveVisibility,
	interface::{catalog::flow::OperatorId, change::Change},
	key::tag::KeyTag,
	operator_with::ApplyWith,
	row::Row,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::{BoxedHostOperator, apply::engine_retention, host::TxnHostContext},
	transaction::{
		ChangeCoordinate, DeferredParams, FlowTransaction,
		deferred::DeferredTransaction,
		substrate::{FlowSubstrate, apply_operator_state},
	},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::flow::operator::{
	MountedOperator, OperatorMetadata, extern_c::binding::operator::ExternCOperatorAdapter,
};
use reifydb_sub_flow::operator::mount::mount;
use reifydb_test_harness::engine::TestEngine;
use reifydb_testing_sdk::{builders::TestChangeBuilder, harness::ExternCOperatorHarness};
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	Result,
	config::ExtensionParams,
	value::{Value, datetime::DateTime, diff_type::DiffType, duration::Duration, row_number::RowNumber},
};

pub struct GuestOperatorHarness<C: MountedOperator + OperatorMetadata + 'static> {
	engine: TestEngine,
	operator: BoxedHostOperator,
	operator_id: OperatorId,
	retention: Option<Duration>,
	version: u64,
	pending: Pending,
	substrate: FlowSubstrate,
	history: Vec<Change>,
	_phantom: PhantomData<C>,
}

impl<C: MountedOperator + OperatorMetadata + 'static> GuestOperatorHarness<C> {
	pub fn builder() -> GuestOperatorHarnessBuilder<C> {
		GuestOperatorHarnessBuilder::new()
	}

	fn begin_txn(&mut self) -> DeferredTransaction {
		let query = self.engine.multi().begin_query().expect("begin_query");
		let state_query = self.engine.multi().begin_query().expect("begin_query");
		let mut txn = DeferredTransaction::new(DeferredParams {
			version: CommitVersion(self.version),
			pending: mem::take(&mut self.pending),
			query: Some(query),
			state_query: Some(state_query),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(1000)),
			substrate: self.substrate.clone(),
		});
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(
				i64::try_from(self.version).expect("test change coordinate fits in i64 millis"),
			)),
		});
		txn
	}

	fn end_txn(&mut self, mut txn: DeferredTransaction) {
		let pending = txn.take_pending();
		apply_operator_state(
			self.substrate.operators.as_ref().expect("the flow harness is built with an operator store"),
			&pending,
		);
		let mut rest = Pending::new();
		for (key, write) in pending.iter_sorted() {
			if matches!(KeyTag::of(key), Some(KeyTag::OperatorState)) {
				continue;
			}
			match write {
				PendingWrite::Set(row) => rest.insert(key.clone(), row.clone()),
				PendingWrite::Remove {
					announce: RemoveVisibility::Announced,
				} => rest.remove(key.clone()),
				PendingWrite::Remove {
					announce: RemoveVisibility::Unobserved,
				} => rest.remove_unobserved(key.clone()),
				PendingWrite::Remove {
					announce: RemoveVisibility::Silent,
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
			let mut host = TxnHostContext::with_retention(&mut txn, operator, self.retention);
			self.operator.apply(&mut host, input)?
		};
		self.end_txn(txn);
		self.history.push(output.clone());
		Ok(output)
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

impl<C: MountedOperator + OperatorMetadata + 'static> Index<usize> for GuestOperatorHarness<C> {
	type Output = Change;

	fn index(&self, index: usize) -> &Self::Output {
		&self.history[index]
	}
}

pub struct GuestOperatorHarnessBuilder<C> {
	params: HashMap<String, Value>,
	with: ApplyWith,
	operator_id: OperatorId,
	version: CommitVersion,
	_phantom: PhantomData<C>,
}

impl<C: MountedOperator + OperatorMetadata + 'static> Default for GuestOperatorHarnessBuilder<C> {
	fn default() -> Self {
		Self::new()
	}
}

impl<C: MountedOperator + OperatorMetadata + 'static> GuestOperatorHarnessBuilder<C> {
	pub fn new() -> Self {
		Self {
			params: HashMap::new(),
			with: ApplyWith::default(),
			operator_id: OperatorId(1),
			version: CommitVersion(1),
			_phantom: PhantomData,
		}
	}

	pub fn with_params<I, K>(mut self, params: I) -> Self
	where
		I: IntoIterator<Item = (K, Value)>,
		K: Into<String>,
	{
		self.params = params.into_iter().map(|(k, v)| (k.into(), v)).collect();
		self
	}

	pub fn add_param(mut self, key: impl Into<String>, value: Value) -> Self {
		self.params.insert(key.into(), value);
		self
	}

	pub fn with(mut self, with: ApplyWith) -> Self {
		self.with = with;
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
			&ExtensionParams::new(<C as OperatorMetadata>::NAME, self.params.clone()),
			&self.with,
		)?;
		let capabilities = <C as OperatorMetadata>::CAPABILITIES;
		let operator = mount(core, self.operator_id, capabilities);

		let substrate = FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		);
		Ok(GuestOperatorHarness {
			engine,
			operator,
			operator_id: self.operator_id,
			retention: engine_retention(&self.with),
			version: self.version.0,
			pending: Pending::new(),
			substrate,
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

fn run_extern_c<C>(params: &[(&str, Value)], with: ApplyWith, inputs: &[Change]) -> Vec<Change>
where
	C: MountedOperator + OperatorMetadata + 'static,
{
	let mut harness = ExternCOperatorHarness::<ExternCOperatorAdapter<C>>::builder()
		.with_params(params.iter().cloned())
		.with(with)
		.build()
		.expect("extern-C harness build");
	inputs.iter().map(|input| harness.apply(input.clone()).expect("extern-C apply")).collect()
}

fn run_guest<C>(params: &[(&str, Value)], with: ApplyWith, inputs: &[Change]) -> Vec<Change>
where
	C: MountedOperator + OperatorMetadata + 'static,
{
	let mut harness = GuestOperatorHarness::<C>::builder()
		.with_params(params.iter().cloned())
		.with(with)
		.build()
		.expect("host harness build");
	inputs.iter().map(|input| harness.apply(input.clone()).expect("host apply")).collect()
}

pub fn assert_backend_parity<C>(params: Vec<(&str, Value)>, with: ApplyWith, scenarios: &[(&str, Vec<Change>)])
where
	C: MountedOperator + OperatorMetadata + 'static,
{
	for (name, inputs) in scenarios {
		let extern_c = run_extern_c::<C>(&params, with.clone(), inputs);
		let host = run_guest::<C>(&params, with.clone(), inputs);

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

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		interface::flow::OperatorCapability,
		key::operator::state::{GroupId, managed_key_in},
		operator_with::WithSpan,
	};
	use reifydb_sdk::{
		error::Result as SdkResult,
		flow::operator::{
			ManagedMount, ManagedOperator,
			column::operator::OperatorColumn,
			context::{ClassState, GuestContext, Managed},
			view::ChangeView,
		},
	};
	use reifydb_testing_sdk::builders::TestRowBuilder;
	use reifydb_value::factory::time::secs;

	use super::*;

	struct ManagedWriter;

	impl OperatorMetadata for ManagedWriter {
		const NAME: &'static str = "managed_writer";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "Writes one managed key per apply";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl ManagedOperator for ManagedWriter {
		fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
			Ok(ManagedWriter)
		}

		fn apply(&mut self, ctx: &mut impl GuestContext<Managed>, _change: impl ChangeView) -> SdkResult<()> {
			let group = GroupId::of(&EncodedKey::new("group".as_bytes()));
			ctx.state().set(&managed_key_in(group, &[]).expect("an empty id fits the keyspace"), &1i64)?;
			Ok(())
		}
	}

	#[test]
	fn a_managed_operator_can_write_state_through_the_harness() {
		// Without a retention the first managed write aborts the harness.
		let with = ApplyWith {
			lateness: Some(WithSpan::Duration(secs(120))),
			..ApplyWith::default()
		};
		let mut harness = GuestOperatorHarness::<ManagedMount<ManagedWriter>>::builder()
			.with(with)
			.build()
			.expect("harness build");

		harness.insert(TestRowBuilder::new(1u64).with_values(vec![Value::Int8(1)]).build());

		assert_eq!(harness.history_len(), 1, "the managed write must complete and be recorded");
	}
}
