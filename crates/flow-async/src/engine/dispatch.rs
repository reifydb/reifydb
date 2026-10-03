// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	flow::{dag::FlowDag, operator::FlowNode},
	interface::{
		catalog::flow::{FlowId, OperatorId},
		change::{Change, ChangeOrigin},
	},
};
use reifydb_value::{Result, reifydb_assertions};
use tracing::{Span, field, instrument};

use crate::{
	engine::FlowEngineInner,
	operator::{
		BoxedHostOperator, InputOrder, guard::enforce_apply_capabilities, host::TxnHostContext,
		sink::BoxedDurableSink,
	},
	transaction::FlowTransaction,
};

pub(super) enum Node<'a> {
	Operator(&'a mut BoxedHostOperator),
	DurableSink(&'a mut BoxedDurableSink),
}

fn order_inbox(operator: &FlowNode, order: InputOrder, mut inbox: Vec<Change>) -> Vec<Change> {
	let arity = operator.inputs.len();
	if order == InputOrder::Declared || arity < 2 {
		return inbox;
	}
	inbox.sort_by_key(|change| match &change.origin {
		ChangeOrigin::Flow(node) => operator
			.inputs
			.iter()
			.position(|input| input == node)
			.map(|position| order.rank(position, arity))
			.unwrap_or(arity),
		_ => arity,
	});
	inbox
}

impl FlowEngineInner {
	pub(super) fn seed_entry_nodes(
		&self,
		flow: &FlowDag,
		flow_id: FlowId,
		change: Change,
		pending: &mut HashMap<OperatorId, Vec<Change>>,
	) {
		match &change.origin {
			ChangeOrigin::Object(source) => {
				if let Some(registrations) = self.sources.get(source) {
					for (registered_flow_id, operator_id) in registrations {
						if *registered_flow_id != flow_id {
							continue;
						}
						if flow.get_operator(operator_id).is_none() {
							continue;
						}
						let routed = Change {
							origin: ChangeOrigin::Flow(*operator_id),
							version: change.version,
							diffs: change.diffs.clone(),
							changed_at: change.changed_at,
						};
						pending.entry(*operator_id).or_default().push(routed);
					}
				}
			}
			ChangeOrigin::Flow(operator_id) => {
				if flow.get_operator(operator_id).is_some() {
					pending.entry(*operator_id).or_default().push(change);
				}
			}
		}
	}

	pub(super) fn dispatch_node<T: FlowTransaction>(
		&mut self,
		txn: &mut T,
		flow_id: FlowId,
		operator: &FlowNode,
		inbox: Vec<Change>,
	) -> Result<Change> {
		let order = self
			.operators
			.get(&(flow_id, operator.id))
			.map(|operator| operator.input_order())
			.unwrap_or(InputOrder::Declared);
		let inbox = order_inbox(operator, order, inbox);
		let merged = Change::merge(inbox)?;
		let version = merged.version;
		let changed_at = merged.changed_at;
		let result = self.apply(txn, flow_id, operator, merged)?;
		let combined = Change::from_flow(operator.id, version, result.diffs, changed_at.max(result.changed_at));
		Ok(combined)
	}

	#[instrument(name = "flow::engine::apply", level = "trace", skip(self, txn, change, operator), fields(
		flow_id = flow_id.0,
		operator_id = operator.id.0,
		node_type = operator.ty.label(),
		num_parents = operator.inputs.len(),
		input_diffs = change.diffs.len(),
		input_rows = field::Empty,
		output_diffs = field::Empty,
		output_rows = field::Empty,
		lock_wait_us = field::Empty,
		apply_time_us = field::Empty
	))]
	fn apply<T: FlowTransaction>(
		&mut self,
		txn: &mut T,
		flow_id: FlowId,
		operator: &FlowNode,
		change: Change,
	) -> Result<Change> {
		let FlowEngineInner {
			operators,
			durable_sinks,
			runtime_context,
			..
		} = self;

		let lock_start = runtime_context.clock.instant();
		let node = match operators.get_mut(&(flow_id, operator.id)) {
			Some(operator) => Node::Operator(operator),
			None => Node::DurableSink(durable_sinks.get_mut(&(flow_id, operator.id)).unwrap()),
		};
		Span::current().record("lock_wait_us", lock_start.elapsed().as_micros() as u64);

		Span::current().record("input_rows", change.row_count());

		let apply_start = runtime_context.clock.instant();
		let result = match node {
			Node::Operator(operator) => {
				enforce_apply_capabilities(operator.id(), operator.capabilities(), &change);
				let mut host = TxnHostContext::with_retention(txn, operator.id(), operator.retention());
				operator.apply(&mut host, change)?
			}
			Node::DurableSink(sink) => {
				enforce_apply_capabilities(sink.id(), sink.capabilities(), &change);
				txn.run_durable_sink(&mut **sink, change)?
			}
		};
		reifydb_assertions! {
			for diff in &result.diffs {
				for batch in diff.pre().into_iter().chain(diff.post()) {
					reifydb_value::value::canonical::assert_canonical_floats(batch, "flow apply");
				}
			}
		}
		Span::current().record("apply_time_us", apply_start.elapsed().as_micros() as u64);
		Span::current().record("output_diffs", result.diffs.len());
		Span::current().record("output_rows", result.row_count());
		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, Float64Array, RecordBatch};
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion},
		flow::operator::{FlowNode, OperatorDef},
		interface::{
			catalog::{
				flow::{FlowId, OperatorId},
				id::ViewId,
			},
			change::{Change, Diff},
			flow::OperatorCapability,
		},
	};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{Result, value::datetime::DateTime};

	use crate::{
		engine::FlowEngineInner,
		operator::{
			HostOperator, host::HostContext, metrics::OperatorSampleRegistry,
			provider::EmptyOperatorProvider,
		},
		transaction::{mock::FlowTxn, substrate::FlowSubstrate},
	};

	const FLOW: FlowId = FlowId(1);
	const OPERATOR: OperatorId = OperatorId(1);

	struct NegativeZeroOperator(fn(RecordBatch) -> Diff);

	impl HostOperator for NegativeZeroOperator {
		fn id(&self) -> OperatorId {
			OPERATOR
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			let column: ArrayRef = Arc::new(Float64Array::from(vec![-0.0f64]));
			let batch = RecordBatch::try_from_iter([("c", column)]).unwrap();
			Ok(Change::from_flow(OPERATOR, change.version, vec![(self.0)(batch)], change.changed_at))
		}
	}

	fn engine_inner(engine: &TestEngine) -> FlowEngineInner {
		FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::with_dictionary(
				engine.inner().dictionary_allocators(),
				engine.inner().operator_state(),
			),
			OperatorSampleRegistry::new(),
		)
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "is not canonical")]
	fn an_operator_output_holding_negative_zero_panics() {
		// Every operator's output passes through apply, so a raw -0.0 must stop here or a compare splits zero.
		let engine = TestEngine::new();
		let mut inner = engine_inner(&engine);
		inner.insert_operator(FLOW, OPERATOR, Box::new(NegativeZeroOperator(Diff::insert)));
		let node = FlowNode::new(
			OPERATOR,
			OperatorDef::SourceView {
				view: ViewId(1),
			},
		);
		let input = Change::from_flow(
			OPERATOR,
			ChangeVersion::from(CommitVersion(1)),
			Vec::<Diff>::new(),
			DateTime::default(),
		);
		let mut txn = engine.flow_txn().deferred();
		let _ = inner.apply(&mut txn, FLOW, &node, input);
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "is not canonical")]
	fn an_operator_removal_holding_negative_zero_panics() {
		// A removal carries its batch in pre only, so a check on post alone must never let its -0.0 through.
		let engine = TestEngine::new();
		let mut inner = engine_inner(&engine);
		inner.insert_operator(FLOW, OPERATOR, Box::new(NegativeZeroOperator(Diff::remove)));
		let node = FlowNode::new(
			OPERATOR,
			OperatorDef::SourceView {
				view: ViewId(1),
			},
		);
		let input = Change::from_flow(
			OPERATOR,
			ChangeVersion::from(CommitVersion(1)),
			Vec::<Diff>::new(),
			DateTime::default(),
		);
		let mut txn = engine.flow_txn().deferred();
		let _ = inner.apply(&mut txn, FLOW, &node, input);
	}
}
