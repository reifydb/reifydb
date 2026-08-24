// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	flow::{FlowId, OperatorId},
	object::ObjectId,
};
use reifydb_rql::flow::flow::FlowDag;

use crate::engine::FlowEngineInner;

impl FlowEngineInner {
	pub fn register_flow_dag(&mut self, flow: FlowDag) {
		self.analyzer.add(flow.clone());
		self.flows.insert(flow.id, flow);
	}

	pub fn add_source(&mut self, flow: FlowId, operator: OperatorId, object: ObjectId) {
		let operators = self.sources.entry(object).or_default();

		let entry = (flow, operator);
		if !operators.contains(&entry) {
			operators.push(entry);
		}
	}

	pub fn add_sink(&mut self, flow: FlowId, operator: OperatorId, sink: ObjectId) {
		let operators = self.sinks.entry(sink).or_default();

		let entry = (flow, operator);
		if !operators.contains(&entry) {
			operators.push(entry);
		}
	}

	pub fn clear(&mut self) {
		self.timers.clear();
		self.operators.clear();
		self.durable_sinks.clear();
		self.flows.clear();
		self.sources.clear();
		self.sinks.clear();
		self.analyzer.clear();
	}

	pub fn remove_flow(&mut self, flow_id: FlowId) {
		let node_ids: Vec<OperatorId> =
			self.flows.get(&flow_id).map(|flow| flow.get_operator_ids().collect()).unwrap_or_default();

		self.timers.remove_flow(flow_id);

		for operator_id in node_ids {
			self.operators.remove(&(flow_id, operator_id));
			self.durable_sinks.remove(&(flow_id, operator_id));
			if let Some(store) = self.substrate.operators.as_ref() {
				store.drop_operator_state(operator_id);
			}
		}

		for entries in self.sources.values_mut() {
			entries.retain(|(fid, _)| *fid != flow_id);
		}
		self.sources.retain(|_, v| !v.is_empty());

		for entries in self.sinks.values_mut() {
			entries.retain(|(fid, _)| *fid != flow_id);
		}
		self.sinks.retain(|_, v| !v.is_empty());

		self.flows.remove(&flow_id);

		self.analyzer.remove(flow_id);
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		common::TimeDomain,
		interface::catalog::id::{SeriesId, ViewId},
		key::operator_state::GroupId,
	};
	use reifydb_rql::flow::operator::{FlowNode, OperatorDef};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		byte_size::ByteSize,
		value::{datetime::DateTime, row_number::RowNumber},
	};

	use super::*;
	use crate::{
		operator::{
			metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider,
			scan::series::SourceSeriesOperator,
		},
		transaction::substrate::FlowSubstrate,
	};

	#[test]
	fn a_repeated_source_registration_leaves_one_entry() {
		// Dispatch fans out one change per registration, so a doubled entry runs the operator twice on it.
		let engine = TestEngine::new();
		let mut inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::new(engine.inner().dictionary_allocators()),
			OperatorSampleRegistry::new(),
		);

		let object = ObjectId::View(ViewId(9));
		inner.add_source(FlowId(1), OperatorId(7), object);
		inner.add_source(FlowId(1), OperatorId(7), object);

		assert_eq!(inner.sources[&object], vec![(FlowId(1), OperatorId(7))]);
	}

	#[test]
	fn removing_a_flow_drops_its_operators_state() {
		// A retired flow gets no other state teardown, so without this drop its bytes stay resident and counted
		// until restart.
		let engine = TestEngine::new();
		let mut inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::with_dictionary(
				engine.inner().dictionary_allocators(),
				engine.inner().operator_state(),
			),
			OperatorSampleRegistry::new(),
		);

		let operator = OperatorId(7);
		let mut builder = FlowDag::builder(FlowId(1));
		builder.add_node(FlowNode::new(
			operator,
			OperatorDef::SourceSeries {
				series: SeriesId(1),
				time_domain: TimeDomain::None,
			},
		));
		inner.register_flow_dag(builder.build());
		inner.insert_operator(FlowId(1), operator, Box::new(SourceSeriesOperator::new(operator)));

		let store = inner.substrate.operators.clone().expect("the test substrate carries an operator store");
		store.set(operator, EncodedKey::new(b"k"), EncodedPodRow::new(&[1u8; 64]));
		assert!(store.bytes(operator) > ByteSize::ZERO, "precondition: the operator's state is resident");

		inner.remove_flow(FlowId(1));

		assert_eq!(store.bytes(operator), ByteSize::ZERO, "the retired operator's state must be dropped");
		assert_eq!(store.total_bytes(), ByteSize::ZERO, "and its bytes must leave the process-wide accounting");
	}

	#[test]
	fn removing_a_flow_drops_its_operators_seal_anchors() {
		// Anchors live outside the key-value rows now, so a drop that spares them leaks a row per sealed group.
		let engine = TestEngine::new();
		let mut inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::with_dictionary(
				engine.inner().dictionary_allocators(),
				engine.inner().operator_state(),
			),
			OperatorSampleRegistry::new(),
		);

		let operator = OperatorId(7);
		let mut builder = FlowDag::builder(FlowId(1));
		builder.add_node(FlowNode::new(
			operator,
			OperatorDef::SourceSeries {
				series: SeriesId(1),
				time_domain: TimeDomain::None,
			},
		));
		inner.register_flow_dag(builder.build());
		inner.insert_operator(FlowId(1), operator, Box::new(SourceSeriesOperator::new(operator)));

		let store = inner.substrate.operators.clone().expect("the test substrate carries an operator store");
		store.anchor_set(operator, GroupId(3), 0, RowNumber(1), DateTime::from_millis(5_000));
		store.anchor_set(operator, GroupId(4), 0, RowNumber(1), DateTime::from_millis(6_000));
		assert!(store.bytes(operator) > ByteSize::ZERO, "precondition: the operator's anchors are resident");

		inner.remove_flow(FlowId(1));

		assert_eq!(store.anchors_by_expiry(operator, GroupId(3), 16), Vec::new());
		assert_eq!(store.anchors_by_expiry(operator, GroupId(4), 16), Vec::new());
		assert_eq!(store.bytes(operator), ByteSize::ZERO, "the retired operator's anchors must be dropped");
		assert_eq!(
			store.total_bytes(),
			ByteSize::ZERO,
			"and their bytes must leave the process-wide accounting"
		);
	}
}
