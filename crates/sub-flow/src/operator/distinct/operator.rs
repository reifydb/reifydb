// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{sync::LazyLock, time::Duration};

use postcard::{from_bytes, to_stdvec};
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	encoded::shape::RowShape,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	internal,
	value::column::columns::Columns,
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::CompileContext,
	},
	vm::stack::SymbolTable,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_sdk::operator::Tick;
use reifydb_value::{Result, error::Error, params::Params, value::blob::Blob};

use crate::{
	operator::{
		Operator, OperatorCell,
		distinct::state::DistinctState,
		stateful::{raw::RawStatefulOperator, row::RowNumberProvider, single::SingleStateful, utils},
	},
	transaction::{FlowTransaction, slot::PersistFn},
};

pub(super) static EMPTY_PARAMS: Params = Params::None;
pub(super) static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

pub struct DistinctOperator {
	parent: OperatorCell,
	pub(super) node: FlowNodeId,
	pub(super) compiled_expressions: Vec<CompiledExpr>,
	pub(super) shape: RowShape,
	pub(super) routines: Routines,
	pub(super) runtime_context: RuntimeContext,
	pub(super) ttl_nanos: Option<u64>,
	pub(super) row_number_provider: RowNumberProvider,
}

impl DistinctOperator {
	pub fn new(
		parent: OperatorCell,
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
			row_number_provider: RowNumberProvider::new(node),
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}

	pub(super) fn load_distinct_state(&self, txn: &mut FlowTransaction) -> Result<DistinctState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(DistinctState::default());
		}

		let blob = self.shape.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(DistinctState::default());
		}

		let state: DistinctState = from_bytes(blob.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize DistinctState: {}", e))))?;
		Ok(state)
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

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_TICK
	}

	fn ticks(&self) -> Option<Duration> {
		self.ticks_interval()
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let node_id = self.node;
		let shape = self.shape.clone();

		let (mut state, persist) = txn.take_operator_state::<DistinctState, _>(node_id, |txn| {
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
					..
				} => {
					let insert_result = self.process_insert(txn, &mut state, &post)?;
					result.extend(insert_result);
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let update_result = self.process_update(txn, &mut state, &pre, &post)?;
					result.extend(update_result);
				}
				Diff::Remove {
					pre,
					..
				} => {
					let remove_result = self.process_remove(txn, &mut state, &pre)?;
					result.extend(remove_result);
				}
			}
		}

		txn.put_operator_state(node_id, state, persist);

		Ok(Change::from_flow(self.node, change.version, result, change.changed_at))
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		self.tick_evict(txn, tick)
	}
}
