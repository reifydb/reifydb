// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, mem};

use reifydb_core::interface::catalog::{
	flow::{FlowId, FlowNodeId},
	id::{RingBufferId, SeriesId, TableId, ViewId},
};
use reifydb_value::value::dictionary::DictionaryId;
use serde::{Deserialize, Serialize};

use crate::flow::{flow::FlowDag, node::FlowNodeType};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ShapeReference {
	Table(TableId),
	View(ViewId),
	RingBuffer(RingBufferId),
	Series(SeriesId),
	Dictionary(DictionaryId),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SinkReference {
	View(ViewId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSummary {
	pub id: FlowId,
	pub sources: Vec<ShapeReference>,
	pub sinks: Vec<SinkReference>,
	pub node_count: usize,
	pub edge_count: usize,
	pub execution_order: Vec<FlowNodeId>,
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
	pub source_dictionaries: BTreeMap<DictionaryId, Vec<FlowId>>,
	pub sink_views: BTreeMap<ViewId, FlowId>,
}

#[derive(Debug, Clone)]
pub struct FlowSchedule {
	pub roots: Vec<FlowId>,
	pub consumers: BTreeMap<FlowId, Vec<FlowId>>,
	pub in_degree: BTreeMap<FlowId, usize>,
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
				source_dictionaries: BTreeMap::new(),
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

	pub fn remove(&mut self, flow_id: FlowId) {
		self.flows.retain(|f| f.id() != flow_id);
		self.dependency_graph = self.calculate();
	}

	fn analyze_flow(flow: &FlowDag) -> FlowSummary {
		let sources = Self::get_sources(flow);
		let sinks = Self::get_sinks(flow);
		let execution_order = flow.topological_order().unwrap_or_default();

		FlowSummary {
			id: flow.id(),
			sources,
			sinks,
			node_count: flow.node_count(),
			edge_count: flow.edge_count(),
			execution_order,
		}
	}

	fn get_sources(flow: &FlowDag) -> Vec<ShapeReference> {
		let mut sources = Vec::new();

		for node_id in flow.get_node_ids() {
			if let Some(node) = flow.get_node(&node_id) {
				match &node.ty {
					FlowNodeType::SourceTable {
						table,
					} => {
						sources.push(ShapeReference::Table(*table));
					}
					FlowNodeType::SourceView {
						view,
					} => {
						sources.push(ShapeReference::View(*view));
					}
					FlowNodeType::SourceRingBuffer {
						ringbuffer,
					} => {
						sources.push(ShapeReference::RingBuffer(*ringbuffer));
					}
					FlowNodeType::SourceSeries {
						series,
					} => {
						sources.push(ShapeReference::Series(*series));
					}
					FlowNodeType::SourceDictionary {
						dictionary,
					} => {
						sources.push(ShapeReference::Dictionary(*dictionary));
					}
					_ => {}
				}
			}
		}

		sources
	}

	fn get_sinks(flow: &FlowDag) -> Vec<SinkReference> {
		let mut sinks = Vec::new();

		for node_id in flow.get_node_ids() {
			if let Some(node) = flow.get_node(&node_id) {
				let view = match &node.ty {
					FlowNodeType::SinkTableView {
						view,
						..
					}
					| FlowNodeType::SinkRingBufferView {
						view,
						..
					}
					| FlowNodeType::SinkSeriesView {
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
		let mut source_dictionaries: BTreeMap<DictionaryId, Vec<FlowId>> = BTreeMap::new();
		let mut sink_views: BTreeMap<ViewId, FlowId> = BTreeMap::new();

		for flow in &self.flows {
			let summary = Self::analyze_flow(flow);

			for source in &summary.sources {
				match source {
					ShapeReference::Table(table_id) => {
						source_tables.entry(*table_id).or_default().push(flow.id());
					}
					ShapeReference::View(view_id) => {
						source_views.entry(*view_id).or_default().push(flow.id());
					}
					ShapeReference::RingBuffer(rb_id) => {
						source_ringbuffers.entry(*rb_id).or_default().push(flow.id());
					}
					ShapeReference::Series(series_id) => {
						source_series.entry(*series_id).or_default().push(flow.id());
					}
					ShapeReference::Dictionary(dict_id) => {
						source_dictionaries.entry(*dict_id).or_default().push(flow.id());
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
			source_dictionaries,
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
				if let ShapeReference::View(view_id) = source
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

	pub fn get_flows_depending_on_table(
		&self,
		dependency_graph: &FlowDependencyGraph,
		table_id: TableId,
	) -> Vec<FlowId> {
		dependency_graph.source_tables.get(&table_id).cloned().unwrap_or_default()
	}

	pub fn get_flows_depending_on_view(
		&self,
		dependency_graph: &FlowDependencyGraph,
		view_id: ViewId,
	) -> Vec<FlowId> {
		dependency_graph.source_views.get(&view_id).cloned().unwrap_or_default()
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
			source_dictionaries: BTreeMap::new(),
			sink_views: BTreeMap::new(),
		};
	}

	pub fn calculate_execution_levels(&self, dependency_graph: &FlowDependencyGraph) -> Vec<Vec<FlowId>> {
		let mut in_degree: BTreeMap<FlowId, usize> = BTreeMap::new();
		let mut adjacency: BTreeMap<FlowId, Vec<FlowId>> = BTreeMap::new();

		for flow_summary in &dependency_graph.flows {
			in_degree.insert(flow_summary.id, 0);
			adjacency.insert(flow_summary.id, Vec::new());
		}

		for dependency in &dependency_graph.dependencies {
			adjacency.entry(dependency.source_flow).or_default().push(dependency.target_flow);
			*in_degree.entry(dependency.target_flow).or_default() += 1;
		}

		let mut levels = Vec::new();
		let mut current_level: Vec<FlowId> =
			in_degree.iter().filter(|&(_, deg)| *deg == 0).map(|(id, _)| *id).collect();

		while !current_level.is_empty() {
			let mut next_level = Vec::new();
			for &flow_id in &current_level {
				if let Some(dependents) = adjacency.get(&flow_id) {
					for &dep in dependents {
						if let Some(deg) = in_degree.get_mut(&dep) {
							*deg -= 1;
							if *deg == 0 {
								next_level.push(dep);
							}
						}
					}
				}
			}
			levels.push(mem::take(&mut current_level));
			current_level = next_level;
		}

		levels
	}

	pub fn calculate_schedule(&self, dependency_graph: &FlowDependencyGraph) -> FlowSchedule {
		let mut in_degree: BTreeMap<FlowId, usize> = BTreeMap::new();
		let mut consumers: BTreeMap<FlowId, Vec<FlowId>> = BTreeMap::new();

		for flow_summary in &dependency_graph.flows {
			in_degree.insert(flow_summary.id, 0);
			consumers.insert(flow_summary.id, Vec::new());
		}

		for dependency in &dependency_graph.dependencies {
			consumers.entry(dependency.source_flow).or_default().push(dependency.target_flow);
			*in_degree.entry(dependency.target_flow).or_default() += 1;
		}

		let roots = in_degree.iter().filter(|&(_, deg)| *deg == 0).map(|(id, _)| *id).collect();

		FlowSchedule {
			roots,
			consumers,
			in_degree,
		}
	}
}

impl Default for FlowGraphAnalyzer {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
pub mod tests {
	use FlowNodeType::{Filter, SinkTableView, SourceTable, SourceView};
	use reifydb_core::{
		common::JoinType,
		interface::catalog::{
			flow::{FlowId, FlowNodeId},
			id::{TableId, ViewId},
		},
	};

	use super::*;
	use crate::flow::{
		flow::FlowDag,
		node::{FlowNode, FlowNodeType},
	};

	fn create_test_flow_with_nodes(id: u64, node_types: Vec<FlowNodeType>) -> FlowDag {
		let mut builder = FlowDag::builder(FlowId(id));

		for (i, node_type) in node_types.into_iter().enumerate() {
			let node = FlowNode::new(FlowNodeId(i as u64 + 1), node_type);
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
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(1));
		assert_eq!(summary.sources, vec![ShapeReference::Table(TableId(100))]);
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
					table: TableId(0),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(2));
		assert_eq!(summary.sources, vec![ShapeReference::View(ViewId(300))]);
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
				},
				SourceView {
					view: ViewId(600),
				},
				FlowNodeType::Join {
					join_type: JoinType::Inner,
					left: vec![],
					right: vec![],
					alias: None,
					snapshot: false,
					natural: false,
				},
				SinkTableView {
					view: ViewId(700),
					table: TableId(0),
				},
				SinkTableView {
					view: ViewId(800),
					table: TableId(0),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(3));
		assert_eq!(summary.sources.len(), 2);
		assert!(summary.sources.contains(&ShapeReference::Table(TableId(500))));
		assert!(summary.sources.contains(&ShapeReference::View(ViewId(600))));
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
				},
				SourceView {
					view: ViewId(200),
				},
				FlowNodeType::SourceInlineData {},
				Filter {
					conditions: vec![],
				},
			],
		);

		let sources = FlowGraphAnalyzer::get_sources(&flow);

		assert_eq!(sources.len(), 2);
		assert!(sources.contains(&ShapeReference::Table(TableId(100))));
		assert!(sources.contains(&ShapeReference::View(ViewId(200))));
	}

	#[test]
	fn test_get_sinks() {
		let flow = create_test_flow_with_nodes(
			5,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
				SinkTableView {
					view: ViewId(300),
					table: TableId(0),
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
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
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
					table: TableId(0),
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
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(101),
				},
				SinkTableView {
					view: ViewId(201),
					table: TableId(0),
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
					table: TableId(0),
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
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow);
		let dependency_graph = analyzer.get_dependency_graph();

		assert_eq!(dependency_graph.flows.len(), 1);
		assert_eq!(dependency_graph.dependencies.len(), 0);
	}

	#[test]
	fn test_get_flows_depending_on_table() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(201),
					table: TableId(0),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceTable {
					table: TableId(101),
				},
				SinkTableView {
					view: ViewId(202),
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		analyzer.add(flow3);
		let dependency_graph = analyzer.get_dependency_graph();

		let flows_using_table_100 = analyzer.get_flows_depending_on_table(dependency_graph, TableId(100));
		assert_eq!(flows_using_table_100.len(), 2);
		assert!(flows_using_table_100.contains(&FlowId(1)));
		assert!(flows_using_table_100.contains(&FlowId(2)));

		let flows_using_table_101 = analyzer.get_flows_depending_on_table(dependency_graph, TableId(101));
		assert_eq!(flows_using_table_101.len(), 1);
		assert!(flows_using_table_101.contains(&FlowId(3)));
	}

	#[test]
	fn test_calculate_execution_levels_linear_chain() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
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
					table: TableId(0),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceView {
					view: ViewId(300),
				},
				SinkTableView {
					view: ViewId(400),
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		analyzer.add(flow3);
		let dependency_graph = analyzer.get_dependency_graph();

		let levels = analyzer.calculate_execution_levels(dependency_graph);

		assert_eq!(levels.len(), 3);
		assert_eq!(levels[0], vec![FlowId(1)]);
		assert_eq!(levels[1], vec![FlowId(2)]);
		assert_eq!(levels[2], vec![FlowId(3)]);
	}

	#[test]
	fn test_calculate_execution_levels_wide_fan_out() {
		let mut analyzer = FlowGraphAnalyzer::new();

		// Flow 1: table -> view 200
		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		);

		// Flows 2,3,4: all read from view 200 (independent of each other)
		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(300),
					table: TableId(0),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(301),
					table: TableId(0),
				},
			],
		);

		let flow4 = create_test_flow_with_nodes(
			4,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(302),
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		analyzer.add(flow3);
		analyzer.add(flow4);
		let dependency_graph = analyzer.get_dependency_graph();

		let levels = analyzer.calculate_execution_levels(dependency_graph);

		assert_eq!(levels.len(), 2);
		assert_eq!(levels[0], vec![FlowId(1)]);
		assert_eq!(levels[1].len(), 3);
		assert!(levels[1].contains(&FlowId(2)));
		assert!(levels[1].contains(&FlowId(3)));
		assert!(levels[1].contains(&FlowId(4)));
	}

	#[test]
	fn test_calculate_execution_levels_independent_roots() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(101),
				},
				SinkTableView {
					view: ViewId(201),
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		let dependency_graph = analyzer.get_dependency_graph();

		let levels = analyzer.calculate_execution_levels(dependency_graph);

		assert_eq!(levels.len(), 1);
		assert_eq!(levels[0].len(), 2);
		assert!(levels[0].contains(&FlowId(1)));
		assert!(levels[0].contains(&FlowId(2)));
	}

	#[test]
	fn test_calculate_schedule_linear_chain() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
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
					table: TableId(0),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceView {
					view: ViewId(300),
				},
				SinkTableView {
					view: ViewId(400),
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		analyzer.add(flow3);
		let dependency_graph = analyzer.get_dependency_graph();

		let schedule = analyzer.calculate_schedule(dependency_graph);

		// only the head of the chain has no producer to wait on
		assert_eq!(schedule.roots, vec![FlowId(1)]);
		assert_eq!(schedule.in_degree[&FlowId(1)], 0);
		assert_eq!(schedule.in_degree[&FlowId(2)], 1);
		assert_eq!(schedule.in_degree[&FlowId(3)], 1);
		// each link unblocks the next flow, the tail unblocks nothing
		assert_eq!(schedule.consumers[&FlowId(1)], vec![FlowId(2)]);
		assert_eq!(schedule.consumers[&FlowId(2)], vec![FlowId(3)]);
		assert!(schedule.consumers[&FlowId(3)].is_empty());
	}

	#[test]
	fn test_calculate_schedule_wide_fan_out() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
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
					table: TableId(0),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(301),
					table: TableId(0),
				},
			],
		);

		let flow4 = create_test_flow_with_nodes(
			4,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(302),
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		analyzer.add(flow3);
		analyzer.add(flow4);
		let dependency_graph = analyzer.get_dependency_graph();

		let schedule = analyzer.calculate_schedule(dependency_graph);

		// the single producer is the only root; each consumer waits on exactly it
		assert_eq!(schedule.roots, vec![FlowId(1)]);
		assert_eq!(schedule.in_degree[&FlowId(2)], 1);
		assert_eq!(schedule.in_degree[&FlowId(3)], 1);
		assert_eq!(schedule.in_degree[&FlowId(4)], 1);
		let mut fan_out = schedule.consumers[&FlowId(1)].clone();
		fan_out.sort();
		assert_eq!(fan_out, vec![FlowId(2), FlowId(3), FlowId(4)]);
	}

	#[test]
	fn test_calculate_schedule_independent_roots() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(101),
				},
				SinkTableView {
					view: ViewId(201),
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		let dependency_graph = analyzer.get_dependency_graph();

		let schedule = analyzer.calculate_schedule(dependency_graph);

		// disconnected flows are both roots and unblock nothing
		let mut roots = schedule.roots.clone();
		roots.sort();
		assert_eq!(roots, vec![FlowId(1), FlowId(2)]);
		assert_eq!(schedule.in_degree[&FlowId(1)], 0);
		assert_eq!(schedule.in_degree[&FlowId(2)], 0);
		assert!(schedule.consumers[&FlowId(1)].is_empty());
		assert!(schedule.consumers[&FlowId(2)].is_empty());
	}

	#[test]
	fn test_calculate_schedule_diamond() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow_a = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		);

		let flow_b = create_test_flow_with_nodes(
			2,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(201),
					table: TableId(0),
				},
			],
		);

		let flow_c = create_test_flow_with_nodes(
			3,
			vec![
				SourceView {
					view: ViewId(200),
				},
				SinkTableView {
					view: ViewId(202),
					table: TableId(0),
				},
			],
		);

		let flow_d = create_test_flow_with_nodes(
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
					table: TableId(0),
				},
			],
		);

		analyzer.add(flow_a);
		analyzer.add(flow_b);
		analyzer.add(flow_c);
		analyzer.add(flow_d);
		let dependency_graph = analyzer.get_dependency_graph();

		let schedule = analyzer.calculate_schedule(dependency_graph);

		// D joins two branches, so it must wait for BOTH producers, not just one level barrier
		assert_eq!(schedule.roots, vec![FlowId(1)]);
		assert_eq!(schedule.in_degree[&FlowId(1)], 0);
		assert_eq!(schedule.in_degree[&FlowId(2)], 1);
		assert_eq!(schedule.in_degree[&FlowId(3)], 1);
		assert_eq!(schedule.in_degree[&FlowId(4)], 2);
		assert_eq!(schedule.consumers[&FlowId(2)], vec![FlowId(4)]);
		assert_eq!(schedule.consumers[&FlowId(3)], vec![FlowId(4)]);
		let mut a_consumers = schedule.consumers[&FlowId(1)].clone();
		a_consumers.sort();
		assert_eq!(a_consumers, vec![FlowId(2), FlowId(3)]);
	}
}
