// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow graph analysis for calculating dependencies and relationships between flows

use std::collections::HashMap;

use reifydb_core::interface::{FlowId, FlowNodeId, TableId, ViewId};
use serde::{Deserialize, Serialize};

use super::{Flow, FlowNodeType};

/// Represents a reference to a data source
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrimitiveReference {
	Table(TableId),
	View(ViewId),
}

/// Represents a reference to a data sink
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SinkReference {
	View(ViewId),
}

/// Summary of a flow's inputs and outputs for frontend rendering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSummary {
	pub id: FlowId,
	pub sources: Vec<PrimitiveReference>,
	pub sinks: Vec<SinkReference>,
	pub node_count: usize,
	pub edge_count: usize,
	pub execution_order: Vec<FlowNodeId>,
}

/// Represents the dependency relationship between flows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDependency {
	pub source_flow: FlowId,
	pub target_flow: FlowId,
	pub via_view: ViewId,
}

/// Complete dependency graph showing relationships between flows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDependencyGraph {
	pub flows: Vec<FlowSummary>,
	pub dependencies: Vec<FlowDependency>,
	pub source_tables: HashMap<TableId, Vec<FlowId>>,
	pub source_views: HashMap<ViewId, Vec<FlowId>>,
	pub sink_views: HashMap<ViewId, FlowId>,
}

pub struct FlowGraphAnalyzer {
	flows: Vec<Flow>,
	dependency_graph: FlowDependencyGraph,
}

impl FlowGraphAnalyzer {
	pub fn new() -> Self {
		Self {
			flows: Vec::new(),
			dependency_graph: FlowDependencyGraph {
				flows: Vec::new(),
				dependencies: Vec::new(),
				source_tables: HashMap::new(),
				source_views: HashMap::new(),
				sink_views: HashMap::new(),
			},
		}
	}

	/// Add a flow to the analyzer
	pub fn add(&mut self, flow: Flow) -> FlowSummary {
		let result = Self::analyze_flow(&flow);
		self.flows.push(flow);
		self.dependency_graph = self.calculate();
		result
	}

	/// Analyze a flow without adding it to the analyzer
	fn analyze_flow(flow: &Flow) -> FlowSummary {
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

	fn get_sources(flow: &Flow) -> Vec<PrimitiveReference> {
		let mut sources = Vec::new();

		for node_id in flow.get_node_ids() {
			if let Some(node) = flow.get_node(&node_id) {
				match &node.ty {
					FlowNodeType::SourceTable {
						table,
					} => {
						sources.push(PrimitiveReference::Table(*table));
					}
					FlowNodeType::SourceView {
						view,
					} => {
						sources.push(PrimitiveReference::View(*view));
					}
					_ => {}
				}
			}
		}

		sources
	}

	fn get_sinks(flow: &Flow) -> Vec<SinkReference> {
		let mut sinks = Vec::new();

		for node_id in flow.get_node_ids() {
			if let Some(node) = flow.get_node(&node_id) {
				if let FlowNodeType::SinkView {
					view,
				} = &node.ty
				{
					sinks.push(SinkReference::View(*view));
				}
			}
		}

		sinks
	}

	/// Get the cached dependency graph
	pub fn get_dependency_graph(&self) -> &FlowDependencyGraph {
		&self.dependency_graph
	}

	fn calculate(&self) -> FlowDependencyGraph {
		let mut flow_summaries = Vec::new();
		let mut source_tables: HashMap<TableId, Vec<FlowId>> = HashMap::new();
		let mut source_views: HashMap<ViewId, Vec<FlowId>> = HashMap::new();
		let mut sink_views: HashMap<ViewId, FlowId> = HashMap::new();

		// First pass: analyze all stored flows and build lookup maps
		for flow in &self.flows {
			let summary = Self::analyze_flow(flow);

			// Track which flows use which tables as sources
			for source in &summary.sources {
				match source {
					PrimitiveReference::Table(table_id) => {
						source_tables.entry(*table_id).or_default().push(flow.id());
					}
					PrimitiveReference::View(view_id) => {
						source_views.entry(*view_id).or_default().push(flow.id());
					}
				}
			}

			// Track which flow produces which view
			for sink in &summary.sinks {
				match sink {
					SinkReference::View(view_id) => {
						sink_views.insert(*view_id, flow.id());
					}
				}
			}

			flow_summaries.push(summary);
		}

		// Second pass: identify dependencies between flows
		let dependencies = self.find_flow_dependencies(&flow_summaries, &sink_views);

		FlowDependencyGraph {
			flows: flow_summaries,
			dependencies,
			source_tables,
			source_views,
			sink_views,
		}
	}

	/// Find dependencies between flows where one flow produces a view that another consumes
	fn find_flow_dependencies(
		&self,
		summaries: &[FlowSummary],
		sink_views: &HashMap<ViewId, FlowId>,
	) -> Vec<FlowDependency> {
		let mut dependencies = Vec::new();

		for flow_summary in summaries {
			for source in &flow_summary.sources {
				if let PrimitiveReference::View(view_id) = source {
					// Check if this view is produced by another flow
					if let Some(&producer_flow_id) = sink_views.get(view_id) {
						// Don't create self-dependencies
						if producer_flow_id != flow_summary.id {
							dependencies.push(FlowDependency {
								source_flow: producer_flow_id,
								target_flow: flow_summary.id,
								via_view: *view_id,
							});
						}
					}
				}
			}
		}

		dependencies
	}

	/// Get all flows that depend on a specific table
	pub fn get_flows_depending_on_table(
		&self,
		dependency_graph: &FlowDependencyGraph,
		table_id: TableId,
	) -> Vec<FlowId> {
		dependency_graph.source_tables.get(&table_id).cloned().unwrap_or_default()
	}

	/// Get all flows that depend on a specific view
	pub fn get_flows_depending_on_view(
		&self,
		dependency_graph: &FlowDependencyGraph,
		view_id: ViewId,
	) -> Vec<FlowId> {
		dependency_graph.source_views.get(&view_id).cloned().unwrap_or_default()
	}

	/// Get the flow that produces a specific view
	pub fn get_flow_producing_view(
		&self,
		dependency_graph: &FlowDependencyGraph,
		view_id: ViewId,
	) -> Option<FlowId> {
		dependency_graph.sink_views.get(&view_id).copied()
	}

	/// Get all stored flows
	pub fn flows(&self) -> &[Flow] {
		&self.flows
	}

	/// Get the number of stored flows
	pub fn flow_count(&self) -> usize {
		self.flows.len()
	}

	/// Clear all stored flows
	pub fn clear(&mut self) {
		self.flows.clear();
		self.dependency_graph = FlowDependencyGraph {
			flows: Vec::new(),
			dependencies: Vec::new(),
			source_tables: HashMap::new(),
			source_views: HashMap::new(),
			sink_views: HashMap::new(),
		};
	}

	/// Calculate the execution order for all flows considering dependencies
	pub fn calculate_execution_order(&self, dependency_graph: &FlowDependencyGraph) -> Vec<FlowId> {
		let mut in_degree: HashMap<FlowId, usize> = HashMap::new();
		let mut adjacency: HashMap<FlowId, Vec<FlowId>> = HashMap::new();

		for flow_summary in &dependency_graph.flows {
			in_degree.insert(flow_summary.id, 0);
			adjacency.insert(flow_summary.id, Vec::new());
		}

		// Build adjacency list and calculate in-degrees
		for dependency in &dependency_graph.dependencies {
			adjacency.entry(dependency.source_flow).or_default().push(dependency.target_flow);
			*in_degree.entry(dependency.target_flow).or_default() += 1;
		}

		// Topological sort using Kahn's algorithm
		let mut queue = Vec::new();
		let mut result = Vec::new();

		// Start with flows that have no dependencies
		for (flow_id, &degree) in &in_degree {
			if degree == 0 {
				queue.push(*flow_id);
			}
		}

		while let Some(flow_id) = queue.pop() {
			result.push(flow_id);

			if let Some(dependents) = adjacency.get(&flow_id) {
				for &dependent_flow in dependents {
					if let Some(degree) = in_degree.get_mut(&dependent_flow) {
						*degree -= 1;
						if *degree == 0 {
							queue.push(dependent_flow);
						}
					}
				}
			}
		}

		result
	}
}

impl Default for FlowGraphAnalyzer {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use FlowNodeType::{Filter, SinkView, SourceTable, SourceView};
	use reifydb_core::interface::{FlowId, FlowNodeId, TableId, ViewId};

	use super::*;
	use crate::flow::{Flow, FlowNode, FlowNodeType};

	fn create_test_flow_with_nodes(id: u64, node_types: Vec<FlowNodeType>) -> Flow {
		let mut builder = Flow::builder(FlowId(id));

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
				SinkView {
					view: ViewId(200),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(1));
		assert_eq!(summary.sources, vec![PrimitiveReference::Table(TableId(100))]);
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
				SinkView {
					view: ViewId(400),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(2));
		assert_eq!(summary.sources, vec![PrimitiveReference::View(ViewId(300))]);
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
					join_type: reifydb_core::JoinType::Inner,
					left: vec![],
					right: vec![],
					alias: None,
				},
				SinkView {
					view: ViewId(700),
				},
				SinkView {
					view: ViewId(800),
				},
			],
		);

		let summary = analyzer.add(flow);

		assert_eq!(summary.id, FlowId(3));
		assert_eq!(summary.sources.len(), 2);
		assert!(summary.sources.contains(&PrimitiveReference::Table(TableId(500))));
		assert!(summary.sources.contains(&PrimitiveReference::View(ViewId(600))));
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
		assert!(sources.contains(&PrimitiveReference::Table(TableId(100))));
		assert!(sources.contains(&PrimitiveReference::View(ViewId(200))));
	}

	#[test]
	fn test_get_sinks() {
		let flow = create_test_flow_with_nodes(
			5,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkView {
					view: ViewId(200),
				},
				SinkView {
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
				},
				SinkView {
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
				SinkView {
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
				},
				SinkView {
					view: ViewId(200),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(101),
				},
				SinkView {
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
				SinkView {
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
				SinkView {
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
	fn test_get_flows_depending_on_table() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkView {
					view: ViewId(200),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkView {
					view: ViewId(201),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceTable {
					table: TableId(101),
				},
				SinkView {
					view: ViewId(202),
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
	fn test_calculate_execution_order() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkView {
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
				SinkView {
					view: ViewId(300),
				},
			],
		);

		let flow3 = create_test_flow_with_nodes(
			3,
			vec![
				SourceView {
					view: ViewId(300),
				},
				SinkView {
					view: ViewId(400),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		analyzer.add(flow3);
		let dependency_graph = analyzer.get_dependency_graph();

		let execution_order = analyzer.calculate_execution_order(dependency_graph);

		assert_eq!(execution_order.len(), 3);
		assert_eq!(execution_order[0], FlowId(1));
		assert_eq!(execution_order[1], FlowId(2));
		assert_eq!(execution_order[2], FlowId(3));
	}

	#[test]
	fn test_parallel_flows_execution_order() {
		let mut analyzer = FlowGraphAnalyzer::new();

		let flow1 = create_test_flow_with_nodes(
			1,
			vec![
				SourceTable {
					table: TableId(100),
				},
				SinkView {
					view: ViewId(200),
				},
			],
		);

		let flow2 = create_test_flow_with_nodes(
			2,
			vec![
				SourceTable {
					table: TableId(101),
				},
				SinkView {
					view: ViewId(201),
				},
			],
		);

		analyzer.add(flow1);
		analyzer.add(flow2);
		let dependency_graph = analyzer.get_dependency_graph();

		let execution_order = analyzer.calculate_execution_order(dependency_graph);

		assert_eq!(execution_order.len(), 2);

		assert!(execution_order.contains(&FlowId(1)));
		assert!(execution_order.contains(&FlowId(2)));
	}
}
