// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::interface::catalog::{
	flow::{FlowId, OperatorId},
	object::ObjectId,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::{Result, value::datetime::DateTime};

use crate::{
	engine::FlowEngineInner, operator::BoxedOperator, transaction::FlowTransaction, window::engine::seal_horizon,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatermarkHold {
	pub object: ObjectId,
	pub frontier: DateTime,
}

pub type WatermarkHolds = Vec<WatermarkHold>;

impl FlowEngineInner {
	pub fn holds<T: FlowTransaction>(&self, txn: &mut T, flow_id: FlowId) -> Result<WatermarkHolds> {
		let Some(flow) = self.flows.get(&flow_id) else {
			return Ok(Vec::new());
		};

		let sinks: Vec<(ObjectId, OperatorId)> = self
			.sinks
			.iter()
			.flat_map(|(object, registrations)| {
				registrations.iter().filter_map(move |(registered, operator)| {
					(*registered == flow_id).then_some((*object, *operator))
				})
			})
			.collect();

		if sinks.is_empty() {
			return Ok(Vec::new());
		}

		let topo = flow.topological_order()?;
		let frontiers = output_frontiers(txn, flow, &self.operators, &topo)?;

		let mut held: WatermarkHolds = Vec::with_capacity(sinks.len());
		for (object, operator_id) in sinks {
			let Some(frontier) = frontiers.get(&operator_id) else {
				continue;
			};
			if frontier.to_millis() == 0 {
				continue;
			}
			held.push(WatermarkHold {
				object,
				frontier: *frontier,
			});
		}
		Ok(held)
	}
}

fn output_frontiers<T: FlowTransaction>(
	txn: &mut T,
	flow: &FlowDag,
	operators: &BTreeMap<OperatorId, BoxedOperator>,
	topo: &[OperatorId],
) -> Result<BTreeMap<OperatorId, DateTime>> {
	let watermarks = txn.source_watermarks();
	let mut computed: BTreeMap<OperatorId, DateTime> = BTreeMap::new();

	for operator_id in topo {
		let Some(node) = flow.get_operator(operator_id) else {
			continue;
		};

		let input = if node.ty.declares_time() {
			Some(watermarks.source_watermark(*operator_id, txn)?)
		} else {
			let mut merged: Option<DateTime> = None;
			for source in &node.inputs {
				let Some(frontier) = computed.get(source).copied() else {
					continue;
				};
				merged = Some(merged.map_or(frontier, |current: DateTime| current.min(frontier)));
			}
			merged
		};

		let Some(input) = input else {
			continue;
		};

		let frontier = operators
			.get(operator_id)
			.and_then(|operator| operator.seal_span())
			.map_or(input, |span| seal_horizon(input, span));

		computed.insert(*operator_id, frontier);
	}

	Ok(computed)
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{
		common::TimeDomain,
		interface::{
			WithEventBus,
			catalog::id::{SeriesId, TableId, ViewId},
			change::Change,
			flow::OperatorCapability,
		},
	};
	use reifydb_rql::flow::{
		flow::FlowBuilder,
		operator::{FlowEdge, FlowNode, OperatorDef},
	};
	use reifydb_runtime::context::{
		RuntimeContext,
		clock::{Clock, MockClock},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{
		factory::time::at_millis,
		value::{duration::Duration, identity::IdentityId},
	};

	use super::*;
	use crate::{
		operator::{
			Operator, bridge::Bridge, metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider,
		},
		transaction::{deferred::DeferredTransaction, substrate::FlowSubstrate},
	};

	const FLOW: FlowId = FlowId(1);

	#[derive(Debug, Clone, Copy)]
	struct WatermarkAdvance {
		source: OperatorId,
		at: DateTime,
	}

	type WatermarkAdvances = Vec<WatermarkAdvance>;

	fn advance(source: u64, at: u64) -> WatermarkAdvance {
		WatermarkAdvance {
			source: OperatorId(source),
			at: at_millis(at),
		}
	}

	fn hold(view: u64, frontier: u64) -> WatermarkHold {
		WatermarkHold {
			object: ObjectId::View(ViewId(view)),
			frontier: at_millis(frontier),
		}
	}

	struct Sealing {
		operator: OperatorId,
		horizon: Option<Duration>,
	}

	impl Operator for Sealing {
		fn id(&self) -> OperatorId {
			self.operator
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _bridge: &mut dyn Bridge, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn seal_span(&self) -> Option<Duration> {
			self.horizon
		}
	}

	fn seconds(seconds: i64) -> Duration {
		Duration::from_milliseconds_const(seconds * 1_000)
	}

	fn engine_inner(engine: &TestEngine) -> FlowEngineInner {
		FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::default(),
			OperatorSampleRegistry::new(),
		)
	}

	fn deferred(engine: &TestEngine) -> DeferredTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		DeferredTransaction::new(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		)
	}

	fn source(id: u64) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SourceSeries {
				series: SeriesId(id),
				time_domain: TimeDomain::Event,
			},
		)
	}

	fn untimed_source(id: u64) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SourceTable {
				table: TableId(id),
				time_domain: TimeDomain::None,
			},
		)
	}

	fn stage(id: u64) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::Map {
				expressions: Vec::new(),
			},
		)
	}

	fn sink(id: u64) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SinkTableView {
				view: ViewId(id),
				table: TableId(id),
			},
		)
	}

	struct Harness {
		engine: TestEngine,
		inner: FlowEngineInner,
		builder: FlowBuilder,
		edges: u64,
	}

	impl Harness {
		fn new() -> Self {
			let engine = TestEngine::new();
			let inner = engine_inner(&engine);
			Self {
				engine,
				inner,
				builder: FlowDag::builder(FLOW),
				edges: 0,
			}
		}

		fn node(mut self, node: FlowNode) -> Self {
			self.builder.add_node(node);
			self
		}

		fn sealing(mut self, id: u64, horizon: Duration) -> Self {
			self.inner.operators.insert(
				OperatorId(id),
				Box::new(Sealing {
					operator: OperatorId(id),
					horizon: Some(horizon),
				}),
			);
			self
		}

		fn edge(mut self, from: u64, to: u64) -> Self {
			self.edges += 1;
			self.builder.add_edge(FlowEdge::new(self.edges, OperatorId(from), OperatorId(to))).unwrap();
			self
		}

		fn registered_sink(mut self, id: u64, flow: FlowId) -> Self {
			self.inner.sinks.insert(ObjectId::View(ViewId(id)), vec![(flow, OperatorId(id))]);
			self
		}

		fn holds(self, advances: WatermarkAdvances) -> WatermarkHolds {
			let Harness {
				engine,
				mut inner,
				builder,
				..
			} = self;
			inner.register_flow_dag(builder.build());
			let mut txn = deferred(&engine);
			let watermarks = txn.source_watermarks();
			for advance in advances {
				watermarks.advance(advance.source, &mut txn, advance.at).unwrap();
			}
			let mut held = inner.holds(&mut txn, FLOW).unwrap();
			held.sort_by_key(|hold| hold.object);
			held
		}
	}

	#[test]
	fn an_operator_that_seals_nothing_hands_the_source_watermark_through_untouched() {
		// A pass-through hop that shaved anything off would make every sink lag its source for free.
		let held = Harness::new()
			.node(source(1))
			.node(stage(2))
			.node(sink(3))
			.edge(1, 2)
			.edge(2, 3)
			.registered_sink(3, FLOW)
			.holds(vec![advance(1, 30_000)]);

		assert_eq!(held, vec![hold(3, 30_000)]);
	}

	#[test]
	fn a_sealing_operator_holds_its_output_back_by_exactly_its_seal_horizon() {
		// Publishing the input watermark past an unsealed window lets a consumer read a bucket the operator may
		// still amend.
		let held = Harness::new()
			.node(source(1))
			.node(stage(2))
			.node(sink(3))
			.edge(1, 2)
			.edge(2, 3)
			.sealing(2, seconds(5))
			.registered_sink(3, FLOW)
			.holds(vec![advance(1, 30_000)]);

		assert_eq!(held, vec![hold(3, 25_000)]);
	}

	#[test]
	fn chained_seal_horizons_accumulate_along_the_path() {
		// Each hop seals against its own input, so the lag must be the sum; a max or a re-read of the source
		// understates it.
		let held = Harness::new()
			.node(source(1))
			.node(stage(2))
			.node(stage(3))
			.node(sink(4))
			.edge(1, 2)
			.edge(2, 3)
			.edge(3, 4)
			.sealing(2, seconds(5))
			.sealing(3, seconds(4))
			.registered_sink(4, FLOW)
			.holds(vec![advance(1, 30_000)]);

		assert_eq!(held, vec![hold(4, 21_000)]);
	}

	#[test]
	fn a_multi_input_operator_is_held_by_its_slowest_input() {
		// An input still behind can emit into a bucket the merged output already claimed sealed.
		let held = Harness::new()
			.node(source(1))
			.node(source(2))
			.node(stage(3))
			.node(sink(4))
			.edge(1, 3)
			.edge(2, 3)
			.edge(3, 4)
			.registered_sink(4, FLOW)
			.holds(vec![advance(1, 30_000), advance(2, 10_000)]);

		assert_eq!(held, vec![hold(4, 10_000)]);
	}

	#[test]
	fn an_untimed_source_is_excluded_from_the_merge_rather_than_pinning_it_at_the_epoch() {
		// A source declaring no time carries no event instant, so counting it as the epoch pins every
		// downstream window open forever.
		let held = Harness::new()
			.node(source(1))
			.node(untimed_source(2))
			.node(stage(3))
			.node(sink(4))
			.edge(1, 3)
			.edge(2, 3)
			.edge(3, 4)
			.registered_sink(4, FLOW)
			.holds(vec![advance(1, 30_000)]);

		assert_eq!(held, vec![hold(4, 30_000)]);
	}

	#[test]
	fn a_flow_whose_every_source_is_untimed_claims_nothing() {
		// With no timed input there is nothing to justify a frontier, so claiming one would seal windows on
		// rows never seen.
		let held = Harness::new()
			.node(untimed_source(1))
			.node(stage(3))
			.node(sink(4))
			.edge(1, 3)
			.edge(3, 4)
			.registered_sink(4, FLOW)
			.holds(vec![]);

		assert!(held.is_empty());
	}

	#[test]
	fn two_branches_of_one_flow_keep_their_own_frontiers() {
		// The branches share no operator, so a flow-wide min would drag the fast sink down for nothing.
		let held = Harness::new()
			.node(source(1))
			.node(sink(2))
			.node(source(3))
			.node(sink(4))
			.edge(1, 2)
			.edge(3, 4)
			.registered_sink(2, FLOW)
			.registered_sink(4, FLOW)
			.holds(vec![advance(1, 30_000), advance(3, 10_000)]);

		assert_eq!(held, vec![hold(2, 30_000), hold(4, 10_000)]);
	}

	#[test]
	fn a_flow_whose_source_never_advanced_claims_nothing() {
		// An unpublished frontier already reads as the epoch, so claiming it must never pass for a real one.
		let held = Harness::new()
			.node(source(1))
			.node(sink(2))
			.edge(1, 2)
			.registered_sink(2, FLOW)
			.holds(Vec::new());

		assert!(held.is_empty());
	}

	#[test]
	fn a_seal_horizon_deeper_than_the_watermark_floors_at_the_epoch_and_claims_nothing() {
		// Subtracting past the epoch must saturate; wrapping would mark every unwritten bucket sealed.
		let held = Harness::new()
			.node(source(1))
			.node(stage(2))
			.node(sink(3))
			.edge(1, 2)
			.edge(2, 3)
			.sealing(2, seconds(5))
			.registered_sink(3, FLOW)
			.holds(vec![advance(1, 3_000)]);

		assert!(held.is_empty());
	}

	#[test]
	fn a_sink_belonging_to_another_flow_is_never_claimed() {
		// The sink registry is shared across flows; claiming a neighbour's sink stamps a frontier no operator
		// here computed.
		let held = Harness::new()
			.node(source(1))
			.node(sink(2))
			.edge(1, 2)
			.registered_sink(2, FlowId(2))
			.holds(vec![advance(1, 30_000)]);

		assert!(held.is_empty());
	}
}
