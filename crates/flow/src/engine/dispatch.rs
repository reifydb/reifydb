// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	interface::{
		catalog::flow::{FlowId, OperatorId},
		change::{Change, ChangeOrigin},
	},
	metrics::point::{PointCounters, census_begin_apply, census_end_apply},
};
use reifydb_rql::flow::{flow::FlowDag, operator::FlowNode};
use reifydb_value::Result;
use tracing::{Span, field, instrument};

use crate::{
	engine::FlowEngineInner,
	operator::{
		BoxedHostOperator, guard::enforce_apply_capabilities, host::TxnHostContext, sink::BoxedDurableSink,
	},
	transaction::FlowTransaction,
};

pub(super) enum Node<'a> {
	Operator(&'a mut BoxedHostOperator),
	DurableSink(&'a mut BoxedDurableSink),
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
		operator: &FlowNode,
		inbox: Vec<Change>,
	) -> Result<Change> {
		let merged = Change::merge(inbox)?;
		let version = merged.version;
		let changed_at = merged.changed_at;
		let result = self.apply(txn, operator, merged)?;
		let combined = Change::from_flow(operator.id, version, result.diffs, changed_at.max(result.changed_at));
		Ok(combined)
	}

	#[instrument(name = "flow::engine::apply", level = "trace", skip(self, txn, change, operator), fields(
		operator_id = operator.id.0,
		node_type = operator.ty.label(),
		num_parents = operator.inputs.len(),
		input_diffs = change.diffs.len(),
		input_rows = field::Empty,
		output_diffs = field::Empty,
		output_rows = field::Empty,
		lock_wait_us = field::Empty,
		apply_time_us = field::Empty,
		state_gets = field::Empty
	))]
	fn apply<T: FlowTransaction>(&mut self, txn: &mut T, operator: &FlowNode, change: Change) -> Result<Change> {
		let FlowEngineInner {
			operators,
			durable_sinks,
			runtime_context,
			..
		} = self;

		let lock_start = runtime_context.clock.instant();
		let node = match operators.get_mut(&operator.id) {
			Some(operator) => Node::Operator(operator),
			None => Node::DurableSink(durable_sinks.get_mut(&operator.id).unwrap()),
		};
		Span::current().record("lock_wait_us", lock_start.elapsed().as_micros() as u64);

		Span::current().record("input_rows", change.row_count());

		let apply_start = runtime_context.clock.instant();
		let gets_before = PointCounters::sample();
		census_begin_apply();
		let result = match node {
			Node::Operator(operator) => {
				enforce_apply_capabilities(operator.id(), operator.capabilities(), &change);
				let mut host = TxnHostContext::new(txn, operator.id());
				operator.apply(&mut host, change)?
			}
			Node::DurableSink(sink) => {
				enforce_apply_capabilities(sink.id(), sink.capabilities(), &change);
				txn.run_durable_sink(&mut **sink, change)?
			}
		};
		Span::current().record("apply_time_us", apply_start.elapsed().as_micros() as u64);
		Span::current().record("state_gets", gets_before.since().gets);
		Span::current().record("output_diffs", result.diffs.len());
		Span::current().record("output_rows", result.row_count());
		census_end_apply(operator.id.0);
		Ok(result)
	}
}
