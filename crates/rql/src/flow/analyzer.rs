// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet};

use reifydb_core::interface::catalog::{
	flow::{FlowId, OperatorId},
	id::{RingBufferId, SeriesId, TableId, ViewId},
	object::ObjectId,
};
use serde::{Deserialize, Serialize};

use crate::flow::{flow::FlowDag, operator::OperatorDef};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectReference {
	Table(TableId),
	View(ViewId),
	RingBuffer(RingBufferId),
	Series(SeriesId),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SinkReference {
	View(ViewId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSummary {
	pub id: FlowId,
	pub sources: Vec<ObjectReference>,
	pub sinks: Vec<SinkReference>,
	pub node_count: usize,
	pub edge_count: usize,
	pub execution_order: Vec<OperatorId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDependency {
	pub source_flow: FlowId,
	pub target_flow: FlowId,
	pub via_view: ViewId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDependencyGraph {
	pub flows: Vec<FlowSummary>,
	pub dependencies: Vec<FlowDependency>,
	pub source_tables: BTreeMap<TableId, Vec<FlowId>>,
	pub source_views: BTreeMap<ViewId, Vec<FlowId>>,
	pub source_ringbuffers: BTreeMap<RingBufferId, Vec<FlowId>>,
	pub source_series: BTreeMap<SeriesId, Vec<FlowId>>,
	pub sink_views: BTreeMap<ViewId, FlowId>,
}

impl FlowDependencyGraph {
	pub fn upstream_closure(&self) -> BTreeMap<ViewId, BTreeSet<ObjectId>> {
		let flows_by_id: BTreeMap<FlowId, &FlowSummary> = self.flows.iter().map(|f| (f.id, f)).collect();

		let mut result = BTreeMap::new();
		for &view in self.sink_views.keys() {
			let mut upstream = BTreeSet::new();
			let mut visited = BTreeSet::new();
			let mut stack = vec![view];

			while let Some(current) = stack.pop() {
				if !visited.insert(current) {
					continue;
				}
				let Some(flow) = self.sink_views.get(&current).and_then(|id| flows_by_id.get(id))
				else {
					continue;
				};
				for source in &flow.sources {
					upstream.insert(object_reference_to_id(source));
					if let ObjectReference::View(v) = source {
						stack.push(*v);
					}
				}
			}

			result.insert(view, upstream);
		}
		result
	}
}

fn object_reference_to_id(reference: &ObjectReference) -> ObjectId {
	match reference {
		ObjectReference::Table(id) => ObjectId::Table(*id),
		ObjectReference::View(id) => ObjectId::View(*id),
		ObjectReference::RingBuffer(id) => ObjectId::RingBuffer(*id),
		ObjectReference::Series(id) => ObjectId::Series(*id),
	}
}

pub struct FlowGraphAnalyzer {
	flows: Vec<FlowDag>,
	dependency_graph: FlowDependencyGraph,
}

impl FlowGraphAnalyzer {
	pub fn new() -> Self {
		Self {
			flows: Vec::new(),
			dependency_graph: FlowDependencyGraph {
				flows: Vec::new(),
				dependencies: Vec::new(),
				source_tables: BTreeMap::new(),
				source_views: BTreeMap::new(),
				source_ringbuffers: BTreeMap::new(),
				source_series: BTreeMap::new(),
				sink_views: BTreeMap::new(),
			},
		}
	}

	pub fn add(&mut self, flow: FlowDag) -> FlowSummary {
		let result = Self::analyze_flow(&flow);
		self.flows.retain(|f| f.id() != flow.id());
		self.flows.push(flow);
		self.dependency_graph = self.calculate();
		result
	}

	pub fn add_all(&mut self, flows: impl IntoIterator<Item = FlowDag>) {
		for flow in flows {
			self.flows.retain(|f| f.id() != flow.id());
			self.flows.push(flow);
		}
		self.dependency_graph = self.calculate();
	}

	pub fn remove(&mut self, flow_id: FlowId) {
		self.flows.retain(|f| f.id() != flow_id);
		self.dependency_graph = self.calculate();
	}

	fn analyze_flow(flow: &FlowDag) -> FlowSummary {
		let sources = Self::get_sources(flow);
		let sinks = Self::get_sinks(flow);
		let execution_order = flow.topological_order().to_vec();

		FlowSummary {
			id: flow.id(),
			sources,
			sinks,
			node_count: flow.node_count(),
			edge_count: flow.edge_count(),
			execution_order,
		}
	}

	fn get_sources(flow: &FlowDag) -> Vec<ObjectReference> {
		let mut sources = Vec::new();

		for node_id in flow.get_operator_ids() {
			if let Some(node) = flow.get_operator(&node_id) {
				match &node.ty {
					OperatorDef::SourceTable {
						table,
						..
					} => {
						sources.push(ObjectReference::Table(*table));
					}
					OperatorDef::SourceView {
						view,
					} => {
						sources.push(ObjectReference::View(*view));
					}
					OperatorDef::SourceRingBuffer {
						ringbuffer,
						..
					} => {
						sources.push(ObjectReference::RingBuffer(*ringbuffer));
					}
					OperatorDef::SourceSeries {
						series,
						..
					} => {
						sources.push(ObjectReference::Series(*series));
					}
					_ => {}
				}
			}
		}

		sources
	}

	fn get_sinks(flow: &FlowDag) -> Vec<SinkReference> {
		let mut sinks = Vec::new();

		for node_id in flow.get_operator_ids() {
			if let Some(node) = flow.get_operator(&node_id) {
				let view = match &node.ty {
					OperatorDef::SinkTableView {
						view,
						..
					}
					| OperatorDef::SinkRingBufferView {
						view,
						..
					}
					| OperatorDef::SinkSeriesView {
						view,
						..
					} => Some(view),
					_ => None,
				};
				if let Some(view) = view {
					sinks.push(SinkReference::View(*view));
				}
			}
		}

		sinks
	}

	pub fn get_dependency_graph(&self) -> &FlowDependencyGraph {
		&self.dependency_graph
	}

	fn calculate(&self) -> FlowDependencyGraph {
		let mut flow_summaries = Vec::new();
		let mut source_tables: BTreeMap<TableId, Vec<FlowId>> = BTreeMap::new();
		let mut source_views: BTreeMap<ViewId, Vec<FlowId>> = BTreeMap::new();
		let mut source_ringbuffers: BTreeMap<RingBufferId, Vec<FlowId>> = BTreeMap::new();
		let mut source_series: BTreeMap<SeriesId, Vec<FlowId>> = BTreeMap::new();
		let mut sink_views: BTreeMap<ViewId, FlowId> = BTreeMap::new();

		for flow in &self.flows {
			let summary = Self::analyze_flow(flow);

			for source in &summary.sources {
				match source {
					ObjectReference::Table(table_id) => {
						source_tables.entry(*table_id).or_default().push(flow.id());
					}
					ObjectReference::View(view_id) => {
						source_views.entry(*view_id).or_default().push(flow.id());
					}
					ObjectReference::RingBuffer(rb_id) => {
						source_ringbuffers.entry(*rb_id).or_default().push(flow.id());
					}
					ObjectReference::Series(series_id) => {
						source_series.entry(*series_id).or_default().push(flow.id());
					}
				}
			}

			for sink in &summary.sinks {
				match sink {
					SinkReference::View(view_id) => {
						sink_views.insert(*view_id, flow.id());
					}
				}
			}

			flow_summaries.push(summary);
		}

		let dependencies = self.find_flow_dependencies(&flow_summaries, &sink_views);

		FlowDependencyGraph {
			flows: flow_summaries,
			dependencies,
			source_tables,
			source_views,
			source_ringbuffers,
			source_series,
			sink_views,
		}
	}

	fn find_flow_dependencies(
		&self,
		summaries: &[FlowSummary],
		sink_views: &BTreeMap<ViewId, FlowId>,
	) -> Vec<FlowDependency> {
		let mut dependencies = Vec::new();

		for flow_summary in summaries {
			for source in &flow_summary.sources {
				if let ObjectReference::View(view_id) = source
					&& let Some(&producer_flow_id) = sink_views.get(view_id)
					&& producer_flow_id != flow_summary.id
				{
					dependencies.push(FlowDependency {
						source_flow: producer_flow_id,
						target_flow: flow_summary.id,
						via_view: *view_id,
					});
				}
			}
		}

		dependencies
	}

	pub fn get_flow_producing_view(
		&self,
		dependency_graph: &FlowDependencyGraph,
		view_id: ViewId,
	) -> Option<FlowId> {
		dependency_graph.sink_views.get(&view_id).copied()
	}

	pub fn flows(&self) -> &[FlowDag] {
		&self.flows
	}

	pub fn flow_count(&self) -> usize {
		self.flows.len()
	}

	pub fn clear(&mut self) {
		self.flows.clear();
		self.dependency_graph = FlowDependencyGraph {
			flows: Vec::new(),
			dependencies: Vec::new(),
			source_tables: BTreeMap::new(),
			source_views: BTreeMap::new(),
			source_ringbuffers: BTreeMap::new(),
			source_series: BTreeMap::new(),
			sink_views: BTreeMap::new(),
		};
	}
}

impl Default for FlowGraphAnalyzer {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
pub mod tests {
	use OperatorDef::{Filter, SinkTableView, SourceTable, SourceView};
	use reifydb_core::{
		common::{JoinType, TimeDomain},
		interface::catalog::{
			flow::{FlowId, OperatorId},
			id::{TableId, ViewId},
		},
	};

	use super::*;
	use crate::flow::{
		flow::FlowDag,
		operator::{FlowNode, OperatorDef},
	};

	fn create_test_flow_with_nodes(id: u64, node_types: Vec<OperatorDef>) -> FlowDag {
		let mut builder = FlowDag::builder(FlowId(id));

		for (i, node_type) in node_types.into_iter().enumerate() {
			let node = FlowNode::new(OperatorId(i as u64 + 1), node_type);
			builder.add_node(node);
		}

		builder.build()
	}

	#[test]
	fn test_analyze_single_flow_with_table_source() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(200),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(1));
		assert_eq!(summary.sources, vec![ObjectReference::Table(TableId(100))]);
		assert_eq!(summary.sinks, vec![SinkReference::View(ViewId(200))]);
		assert_eq!(summary.node_count, 2);
		assert_eq!(analyzer.flow_count(), 1);
	}

	#[test]
	fn test_analyze_single_flow_with_view_source() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow = create_test_flow_with_nodes(
			2,
			vec![
				SourceView {
					view: ViewId(300),
				},
				Filter {
					conditions: vec![],
				},
				SinkTableView {
					view: ViewId(400),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(2));
		assert_eq!(summary.sources, vec![ObjectReference::View(ViewId(300))]);
		assert_eq!(summary.sinks, vec![SinkReference::View(ViewId(400))]);
		assert_eq!(summary.node_count, 3);
		assert_eq!(analyzer.flow_count(), 1);
	}

	#[test]
	fn test_analyze_flow_with_multiple_sources_and_sinks() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow = create_test_flow_with_nodes(
			3,
			vec![
				SourceTable {
					table: TableId(500),
					time_domain: TimeDomain::None,
				},
				SourceView {
					view: ViewId(600),
				},
				OperatorDef::Join {
					join_type: JoinType::Inner,
					left: vec![],
					right: vec![],
					alias: None,
					snapshot: false,
					natural: false,
					pick: None,
				},
				SinkTableView {
					view: ViewId(700),
				},
				SinkTableView {
					view: ViewId(800),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(3));
		assert_eq!(summary.sources.len(), 2);
		assert!(summary.sources.contains(&ObjectReference::Table(TableId(500))));
		assert!(summary.sources.contains(&ObjectReference::View(ViewId(600))));
		assert_eq!(summary.sinks.len(), 2);
		assert!(summary.sinks.contains(&SinkReference::View(ViewId(700))));
		assert!(summary.sinks.contains(&SinkReference::View(ViewId(800))));
	}

	#[test]
	fn test_get_sources() {
		let flow = create_test_flow_with_nodes(
			4,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SourceView {
					view: ViewId(200),
				},
				OperatorDef::SourceInlineData {},
				Filter {
					conditions: vec![],
				},
			],
		);

		let sources = FlowGraphAnalyzer::get_sources(&flow);

		assert_eq!(sources.len(), 2);
		assert!(sources.contains(&ObjectReference::Table(TableId(100))));
		assert!(sources.contains(&ObjectReference::View(ViewId(200))));
	}

	#[test]
	fn test_get_sinks() {
		let flow = create_test_flow_with_nodes(
			5,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(300),
				},
			],
		);

		let sinks = FlowGraphAnalyzer::get_sinks(&flow);

		assert_eq!(sinks.len(), 2);
		assert!(sinks.contains(&SinkReference::View(ViewId(200))));
		assert!(sinks.contains(&SinkReference::View(ViewId(300))));
	}

	#[test]
	fn test_calculate_dependency_graph_simple() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(200),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(300),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		let dependency_graph = analyzer.get_dependency_graph();

		assert_eq!(dependency_graph.flows.len(), 2);
		assert_eq!(dependency_graph.dependencies.len(), 1);

		let dependency = &dependency_graph.dependencies[0];
		assert_eq!(dependency.source_flow, FlowId(1));
		assert_eq!(dependency.target_flow, FlowId(2));
		assert_eq!(dependency.via_view, ViewId(200));

		assert_eq!(dependency_graph.source_tables.get(&TableId(100)).unwrap(), &vec![FlowId(1)]);
		assert_eq!(dependency_graph.source_views.get(&ViewId(200)).unwrap(), &vec![FlowId(2)]);
		assert_eq!(dependency_graph.sink_views.get(&ViewId(200)).unwrap(), &FlowId(1));
		assert_eq!(dependency_graph.sink_views.get(&ViewId(300)).unwrap(), &FlowId(2));
	}

	#[test]
	fn test_calculate_dependency_graph_complex() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(200),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(101),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(201),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SourceView {
					view: ViewId(201),
				},
				SinkTableView {
					view: ViewId(300),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		analyzer.add(flow3);
		let dependency_graph = analyzer.get_dependency_graph();

		assert_eq!(dependency_graph.flows.len(), 3);
		assert_eq!(dependency_graph.dependencies.len(), 2);

		let mut dependencies_found = 0;
		for dependency in &dependency_graph.dependencies {
			if dependency.target_flow == FlowId(3) {
				dependencies_found += 1;
				assert!(dependency.source_flow == FlowId(1) || dependency.source_flow == FlowId(2));
				assert!(dependency.via_view == ViewId(200) || dependency.via_view == ViewId(201));
			}
		}
		assert_eq!(dependencies_found, 2);
	}

	#[test]
	fn test_no_self_dependencies() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow = create_test_flow_with_nodes(
			1,
			vec![
				SourceView {
					view: ViewId(100),
				},
				SinkTableView {
					view: ViewId(100),
				},
			],
		);

		analyzer.add(flow);
		let dependency_graph = analyzer.get_dependency_graph();

		assert_eq!(dependency_graph.flows.len(), 1);
		assert_eq!(dependency_graph.dependencies.len(), 0);
	}

	#[test]
	fn a_source_table_indexes_every_flow_that_reads_it() {
		// Dispatch routes a change by this map, so a missed flow never runs on writes to its own source.
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(200),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(201),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceTable {
					table: TableId(101),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(202),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		analyzer.add(flow3);
		let dependency_graph = analyzer.get_dependency_graph();

		let flows_using_table_100 = &dependency_graph.source_tables[&TableId(100)];
		assert_eq!(flows_using_table_100.len(), 2);
		assert!(flows_using_table_100.contains(&FlowId(1)));
		assert!(flows_using_table_100.contains(&FlowId(2)));

		let flows_using_table_101 = &dependency_graph.source_tables[&TableId(101)];
		assert_eq!(flows_using_table_101.len(), 1);
		assert!(flows_using_table_101.contains(&FlowId(3)));

		assert!(!dependency_graph.source_tables.contains_key(&TableId(999)));
	}

	#[test]
	fn test_upstream_closure_chain() {
		let mut analyzer = FlowGraphAnalyzer::new();
		analyzer.add(create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(200),
				},
			],
		));
		analyzer.add(create_test_flow_with_nodes(
			2,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(300),
				},
			],
		));

		let closure = analyzer.get_dependency_graph().upstream_closure();

		assert_eq!(
			closure[&ViewId(200)],
			BTreeSet::from([ObjectId::Table(TableId(100))]),
			"direct source only"
		);
		assert_eq!(
			closure[&ViewId(300)],
			BTreeSet::from([ObjectId::Table(TableId(100)), ObjectId::View(ViewId(200))]),
			"transitive closure must reach through the intermediate view to the table"
		);
	}

	#[test]
	fn test_upstream_closure_diamond() {
		// The diamond reconverges, so the closure must visit the shared upstream once, not twice.
		let mut analyzer = FlowGraphAnalyzer::new();
		analyzer.add(create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(200),
				},
			],
		));
		analyzer.add(create_test_flow_with_nodes(
			2,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(201),
				},
			],
		));
		analyzer.add(create_test_flow_with_nodes(
			3,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(202),
				},
			],
		));
		analyzer.add(create_test_flow_with_nodes(
			4,
			vec![
				SourceView {
					view: ViewId(201),
				},
				SourceView {
					view: ViewId(202),
				},
				SinkTableView {
					view: ViewId(203),
				},
			],
		));

		let closure = analyzer.get_dependency_graph().upstream_closure();

		assert_eq!(
			closure[&ViewId(203)],
			BTreeSet::from([
				ObjectId::Table(TableId(100)),
				ObjectId::View(ViewId(200)),
				ObjectId::View(ViewId(201)),
				ObjectId::View(ViewId(202)),
			]),
			"diamond must merge both branches and dedupe the shared root"
		);
	}

	#[test]
	fn test_upstream_closure_stops_at_unregistered_producer() {
		// A view with no producing flow in this graph is an async boundary: the walk records it as a leaf and
		// must not reach the objects behind it.
		let mut analyzer = FlowGraphAnalyzer::new();
		analyzer.add(create_test_flow_with_nodes(
			1,
			vec![
				SourceView {
					view: ViewId(900),
				},
				SinkTableView {
					view: ViewId(300),
				},
			],
		));

		let closure = analyzer.get_dependency_graph().upstream_closure();

		assert_eq!(
			closure[&ViewId(300)],
			BTreeSet::from([ObjectId::View(ViewId(900))]),
			"an unregistered producer is an async boundary, included only as a leaf"
		);
		assert!(!closure.contains_key(&ViewId(900)), "views this graph does not produce get no entry");
	}

	#[test]
	fn test_upstream_closure_cycle_terminates() {
		// The two flows produce each other's source, so only the visited set stops the walk looping.
		let mut analyzer = FlowGraphAnalyzer::new();
		analyzer.add(create_test_flow_with_nodes(
			1,
			vec![
				SourceView {
					view: ViewId(300),
				},
				SinkTableView {
					view: ViewId(200),
				},
			],
		));
		analyzer.add(create_test_flow_with_nodes(
			2,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(300),
				},
			],
		));

		let closure = analyzer.get_dependency_graph().upstream_closure();

		assert_eq!(
			closure[&ViewId(200)],
			BTreeSet::from([ObjectId::View(ViewId(200)), ObjectId::View(ViewId(300))])
		);
		assert_eq!(
			closure[&ViewId(300)],
			BTreeSet::from([ObjectId::View(ViewId(200)), ObjectId::View(ViewId(300))])
		);
	}
}
