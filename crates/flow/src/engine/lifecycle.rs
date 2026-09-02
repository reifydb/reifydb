// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	flow::{FlowId, OperatorId},
	object::ObjectId,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::reifydb_assertions;

use crate::engine::FlowEngineInner;

impl FlowEngineInner {
	pub fn register_flow_dag(&mut self, flow: FlowDag) {
		reifydb_assertions! {
			assert!(
				self.flows.values().all(|registered| registered.ephemeral == flow.ephemeral),
				"an engine holding both durable and ephemeral flows keys two different operators \
				 under the same flow and operator id, and dropping one takes the other's state"
			);
		}
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
		let flow = self.flows.get(&flow_id);
		let node_ids: Vec<OperatorId> = flow.map(|flow| flow.get_operator_ids().collect()).unwrap_or_default();
		let ephemeral = flow.is_some_and(|flow| flow.ephemeral);

		self.timers.remove_flow(flow_id);

		for operator_id in node_ids {
			self.operators.remove(&(flow_id, operator_id));
			self.durable_sinks.remove(&(flow_id, operator_id));
			if ephemeral {
				continue;
			}
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

	use reifydb_codec::row::{operator::state::OperatorState, pod::EncodedPodRow};
	use reifydb_core::{
		common::TimeDomain,
		interface::catalog::id::{SeriesId, ViewId},
		key::operator::{
			keyspace::join::{JoinRowExpiryState as JoinRowExpiry, join_expiry_due_key},
			state::{GroupId, KeyspaceId, custom_not_cached_key},
		},
	};
	use reifydb_rql::flow::operator::{FlowNode, OperatorDef};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_store_operator::{store::OperatorStore, types::OperatorWrite};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		byte_size::ByteSize,
		util::hash::Hash128,
		value::{datetime::DateTime, row_number::RowNumber},
	};

	use super::*;
	use crate::{
		operator::{
			metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider,
			scan::series::SourceSeriesOperator,
		},
		transaction::{join_expiry::join_expiry_key, substrate::FlowSubstrate},
	};

	fn occupied_keyspaces(store: &OperatorStore, operator: OperatorId) -> Vec<KeyspaceId> {
		// Census order follows the inverted keyspace byte, so a stable comparison has to sort by the raw id.
		let mut keyspaces: Vec<KeyspaceId> = store
			.census()
			.into_iter()
			.filter(|entry| entry.operator == operator && entry.keys > 0)
			.map(|entry| entry.keyspace)
			.collect();
		keyspaces.sort_by_key(|keyspace| keyspace.0);
		keyspaces
	}

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
		store.apply_batch(&[OperatorWrite::Insert {
			operator,
			key: custom_not_cached_key(b"k")
				.expect("a fixture name must fit the keyspace's id width")
				.into_encoded(),
			post: EncodedPodRow::new(&[1u8; 64]),
		}]);
		assert!(store.bytes(operator) > ByteSize::ZERO, "precondition: the operator's state is resident");

		inner.remove_flow(FlowId(1));

		assert_eq!(store.bytes(operator), ByteSize::ZERO, "the retired operator's state must be dropped");
		assert_eq!(store.total_bytes(), ByteSize::ZERO, "and its bytes must leave the process-wide accounting");
	}

	#[test]
	fn removing_a_flow_drops_both_keyspaces_its_join_expiries_occupy() {
		// The due index sits at root, outside every group partition, so only an operator drop can take it.
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
		let at = DateTime::from_millis(5_000);
		store.apply_batch(&[
			OperatorWrite::Insert {
				operator,
				key: join_expiry_key(GroupId::hashed(Hash128(3)), 0, RowNumber(1)).into_encoded(),
				post: JoinRowExpiry {
					at,
				}
				.encode_state()
				.expect("a join expiry payload must encode"),
			},
			OperatorWrite::Insert {
				operator,
				key: join_expiry_due_key(at, GroupId::hashed(Hash128(3)), 0, RowNumber(1))
					.into_encoded(),
				post: EncodedPodRow::new(&[]),
			},
		]);
		assert_eq!(
			occupied_keyspaces(&store, operator),
			vec![KeyspaceId::JOIN_ROW_EXPIRY, KeyspaceId::JOIN_EXPIRY_DUE],
			"precondition: an arm occupies a group scoped keyspace and a root scoped one"
		);

		inner.remove_flow(FlowId(1));

		assert_eq!(
			occupied_keyspaces(&store, operator),
			Vec::new(),
			"the retired operator must be left holding neither keyspace"
		);
		assert_eq!(
			store.bytes(operator),
			ByteSize::ZERO,
			"the retired operator's join expiries must be dropped"
		);
		assert_eq!(
			store.total_bytes(),
			ByteSize::ZERO,
			"and their bytes must leave the process-wide accounting"
		);
	}

	#[test]
	fn removing_one_flow_spares_another_flows_operator_of_the_same_id() {
		// Every ephemeral flow numbers from 1, so keyed by the operator alone one retirement unregisters every
		// namesake.
		let engine = TestEngine::new();
		let mut inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::new(engine.inner().dictionary_allocators()),
			OperatorSampleRegistry::new(),
		);

		let operator = OperatorId(1);
		for flow_id in [FlowId(1), FlowId(2)] {
			let mut builder = FlowDag::builder(flow_id);
			builder.ephemeral();
			builder.add_node(FlowNode::new(
				operator,
				OperatorDef::SourceSeries {
					series: SeriesId(1),
					time_domain: TimeDomain::None,
				},
			));
			inner.register_flow_dag(builder.build());
			inner.insert_operator(flow_id, operator, Box::new(SourceSeriesOperator::new(operator)));
		}

		inner.remove_flow(FlowId(1));

		assert!(inner.operator(FlowId(1), operator).is_none(), "the retired flow's operator must be gone");
		assert!(
			inner.operator(FlowId(2), operator).is_some(),
			"the surviving flow's operator shares only the id, so it must still be registered"
		);
	}

	#[test]
	fn removing_an_ephemeral_flow_leaves_the_shared_operator_state_untouched() {
		// An ephemeral flow owns no shared state, so dropping the id it reuses takes a durable operator's
		// instead.
		let engine = TestEngine::new();
		let store = engine.inner().operator_state();

		let operator = OperatorId(1);
		store.apply_batch(&[OperatorWrite::Insert {
			operator,
			key: custom_not_cached_key(b"k")
				.expect("a fixture name must fit the keyspace's id width")
				.into_encoded(),
			post: EncodedPodRow::new(&[1u8; 64]),
		}]);
		let durable_bytes = store.bytes(operator);
		assert!(durable_bytes > ByteSize::ZERO, "precondition: the durable operator's state is resident");

		let mut inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::new(engine.inner().dictionary_allocators()),
			OperatorSampleRegistry::new(),
		);

		let mut builder = FlowDag::builder(FlowId(1));
		builder.ephemeral();
		builder.add_node(FlowNode::new(
			operator,
			OperatorDef::SourceSeries {
				series: SeriesId(1),
				time_domain: TimeDomain::None,
			},
		));
		inner.register_flow_dag(builder.build());
		inner.insert_operator(FlowId(1), operator, Box::new(SourceSeriesOperator::new(operator)));

		inner.remove_flow(FlowId(1));

		assert_eq!(
			store.bytes(operator),
			durable_bytes,
			"the durable operator's state must survive an unrelated flow retiring under its id"
		);
	}
}
