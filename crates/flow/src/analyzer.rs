// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet};

use reifydb_core::{
	common::JoinType,
	flow::{
		dag::FlowDag,
		operator::{LookupObject, OperatorDef},
	},
	interface::catalog::{
		flow::{FlowId, OperatorId},
		id::{RingBufferId, SeriesId, TableId, ViewId},
		object::ObjectId,
	},
	operator_with::Emit,
};
use serde::{Deserialize, Serialize};

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
	pub collapsible_operators: bool,
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
	pub lookup_views: BTreeMap<ViewId, Vec<FlowId>>,
	pub exact_lookup_views: BTreeMap<ViewId, Vec<FlowId>>,
	pub from_views: BTreeMap<ViewId, Vec<FlowId>>,
}

impl FlowDependencyGraph {
	pub fn upstream_closure(&self) -> BTreeMap<ViewId, BTreeSet<ObjectId>> {
		let flows_by_id: BTreeMap<FlowId, &FlowSummary> = self.flows.iter().map(|f| (f.id, f)).collect();
		let lookups_by_flow = lookups_by_flow(&self.lookup_views);

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
				for &v in lookups_by_flow.get(&flow.id).into_iter().flatten() {
					upstream.insert(ObjectId::View(v));
					stack.push(v);
				}
			}

			result.insert(view, upstream);
		}
		result
	}
}

fn lookups_by_flow(lookup_views: &BTreeMap<ViewId, Vec<FlowId>>) -> BTreeMap<FlowId, Vec<ViewId>> {
	let mut result: BTreeMap<FlowId, Vec<ViewId>> = BTreeMap::new();
	for (&view, readers) in lookup_views {
		for &reader in readers {
			result.entry(reader).or_default().push(view);
		}
	}
	result
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
				lookup_views: BTreeMap::new(),
				exact_lookup_views: BTreeMap::new(),
				from_views: BTreeMap::new(),
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
		let collapsible_operators = flow.get_operator_ids().into_iter().all(|node_id| {
			flow.get_operator(&node_id).is_some_and(|node| match &node.ty {
				OperatorDef::SourceTable {
					..
				}
				| OperatorDef::SourceView {
					..
				}
				| OperatorDef::SourceRingBuffer {
					..
				}
				| OperatorDef::Filter {
					..
				}
				| OperatorDef::Map {
					..
				}
				| OperatorDef::Extend {
					..
				}
				| OperatorDef::SinkTableView {
					..
				} => true,
				OperatorDef::Join {
					join_type,
					with,
					..
				} => {
					(with.emit != Emit::Off || !with.exact)
						&& with.retention.is_none()
						&& !(*join_type == JoinType::Left && with.emit != Emit::Off)
				}
				OperatorDef::Lookup {
					with,
					..
				} => (with.emit != Emit::Off || !with.exact) && with.retention.is_none(),
				_ => false,
			})
		});

		FlowSummary {
			id: flow.id(),
			sources,
			sinks,
			node_count: flow.node_count(),
			edge_count: flow.edge_count(),
			execution_order,
			collapsible_operators,
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
					}
					| OperatorDef::SinkQueueView {
						view,
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

	fn get_lookup_views(flow: &FlowDag) -> BTreeSet<ViewId> {
		let mut views = BTreeSet::new();

		for node_id in flow.get_operator_ids() {
			if let Some(node) = flow.get_operator(&node_id)
				&& let OperatorDef::Lookup {
					right: LookupObject::View(view),
					..
				} = &node.ty
			{
				views.insert(*view);
			}
		}

		views
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
		let mut lookup_views: BTreeMap<ViewId, Vec<FlowId>> = BTreeMap::new();
		let mut exact_lookup_views: BTreeMap<ViewId, Vec<FlowId>> = BTreeMap::new();
		let mut from_views: BTreeMap<ViewId, Vec<FlowId>> = BTreeMap::new();

		for flow in &self.flows {
			let summary = Self::analyze_flow(flow);

			for view_id in Self::get_lookup_views(flow) {
				lookup_views.entry(view_id).or_default().push(flow.id());
			}

			let exact: BTreeSet<ViewId> = flow
				.get_operator_ids()
				.into_iter()
				.filter_map(|node_id| match flow.get_operator(&node_id).map(|node| &node.ty) {
					Some(OperatorDef::Lookup {
						right: LookupObject::View(view),
						with,
						..
					}) if with.exact => Some(*view),
					_ => None,
				})
				.collect();
			for view_id in exact {
				exact_lookup_views.entry(view_id).or_default().push(flow.id());
			}

			let lookup_rights: BTreeSet<OperatorId> = flow
				.get_operator_ids()
				.into_iter()
				.filter_map(|node_id| flow.get_operator(&node_id))
				.filter(|node| matches!(node.ty, OperatorDef::Lookup { .. }))
				.filter_map(|node| node.inputs.get(1).copied())
				.collect();
			let read_from: BTreeSet<ViewId> = flow
				.get_operator_ids()
				.into_iter()
				.filter(|node_id| !lookup_rights.contains(node_id))
				.filter_map(|node_id| match flow.get_operator(&node_id).map(|node| &node.ty) {
					Some(OperatorDef::SourceView {
						view,
					}) => Some(*view),
					_ => None,
				})
				.collect();
			for view_id in read_from {
				from_views.entry(view_id).or_default().push(flow.id());
			}

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

		let dependencies = self.find_flow_dependencies(&flow_summaries, &sink_views, &lookup_views);

		FlowDependencyGraph {
			flows: flow_summaries,
			dependencies,
			source_tables,
			source_views,
			source_ringbuffers,
			source_series,
			sink_views,
			lookup_views,
			exact_lookup_views,
			from_views,
		}
	}

	fn find_flow_dependencies(
		&self,
		summaries: &[FlowSummary],
		sink_views: &BTreeMap<ViewId, FlowId>,
		lookup_views: &BTreeMap<ViewId, Vec<FlowId>>,
	) -> Vec<FlowDependency> {
		let mut dependencies = Vec::new();
		let lookups_by_flow = lookups_by_flow(lookup_views);

		for flow_summary in summaries {
			let source_views = flow_summary.sources.iter().filter_map(|source| match source {
				ObjectReference::View(view_id) => Some(view_id),
				_ => None,
			});
			let read_views =
				source_views.chain(lookups_by_flow.get(&flow_summary.id).into_iter().flatten());
			for view_id in read_views {
				if let Some(&producer_flow_id) = sink_views.get(view_id)
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
			lookup_views: BTreeMap::new(),
			exact_lookup_views: BTreeMap::new(),
			from_views: BTreeMap::new(),
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
		common::{JoinType, TimeDomain, WindowKind, WindowSize},
		flow::{
			dag::FlowDag,
			operator::{FlowNode, OperatorDef},
		},
		interface::catalog::{
			flow::{FlowEdge, FlowId, OperatorId},
			id::{RingBufferId, TableId, ViewId},
		},
		operator_with::{AggregateWith, ApplyWith, Emit, JoinWith, LookupWith, WindowWith},
		row::{JoinPick, JoinRetention, OperatorRetention},
		sort::{SortDirection, SortKey},
	};
	use reifydb_value::{fragment::Fragment, value::duration::Duration};

	use super::*;

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
		assert_eq!(analyzer.flows().len(), 1);
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
		assert_eq!(analyzer.flows().len(), 1);
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
					natural: false,
					with: JoinWith::default(),
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

	fn lookup(right: LookupObject) -> OperatorDef {
		OperatorDef::Lookup {
			join_type: JoinType::Inner,
			right,
			left: vec![],
			alias: None,
			with: LookupWith::default(),
		}
	}

	fn lookup_reader_and_producer() -> FlowGraphAnalyzer {
		let mut analyzer = FlowGraphAnalyzer::new();
		analyzer.add(create_test_flow_with_nodes(
			10,
			vec![
				SourceTable {
					table: TableId(100),
					time_domain: TimeDomain::None,
				},
				SinkTableView {
					view: ViewId(500),
				},
			],
		));
		analyzer.add(create_test_flow_with_nodes(
			20,
			vec![
				SourceTable {
					table: TableId(200),
					time_domain: TimeDomain::None,
				},
				lookup(LookupObject::Table(TableId(300))),
				lookup(LookupObject::View(ViewId(500))),
				lookup(LookupObject::View(ViewId(500))),
				SinkTableView {
					view: ViewId(600),
				},
			],
		));
		analyzer
	}

	#[test]
	fn test_lookup_view_is_listed_once_per_reader_and_never_as_a_source() {
		// A lookup view in source_views would route its commit log into the reader, which MD32 forbids.
		let analyzer = lookup_reader_and_producer();
		let graph = analyzer.get_dependency_graph();

		assert_eq!(
			graph.lookup_views.get(&ViewId(500)),
			Some(&vec![FlowId(20)]),
			"two lookups on one view list the reader once"
		);
		assert!(!graph.source_views.contains_key(&ViewId(500)), "a lookup view must stay out of source_views");
		assert!(
			!graph.source_tables.contains_key(&TableId(300)),
			"a lookup table must stay out of source_tables"
		);
		assert_eq!(graph.lookup_views.len(), 1, "a table read by a lookup is not a lookup view");
	}

	#[test]
	fn test_lookup_view_makes_its_producer_an_upstream_dependency() {
		// Without the dependency the reader could run ahead of the flow that writes the view it reads.
		let analyzer = lookup_reader_and_producer();
		let graph = analyzer.get_dependency_graph();

		let via_lookup: Vec<_> = graph.dependencies.iter().filter(|d| d.via_view == ViewId(500)).collect();
		assert_eq!(via_lookup.len(), 1, "one dependency per lookup view and reader: {:?}", graph.dependencies);
		assert_eq!(via_lookup[0].source_flow, FlowId(10));
		assert_eq!(via_lookup[0].target_flow, FlowId(20));
	}

	#[test]
	fn test_upstream_closure_walks_through_a_lookup_view() {
		// A read of the reader's view must see the lookup view and everything behind it as upstream.
		let analyzer = lookup_reader_and_producer();

		let closure = analyzer.get_dependency_graph().upstream_closure();

		assert_eq!(
			closure[&ViewId(600)],
			BTreeSet::from([
				ObjectId::Table(TableId(200)),
				ObjectId::View(ViewId(500)),
				ObjectId::Table(TableId(100)),
			]),
			"the lookup view and its producer's source are upstream; the looked-up table is not"
		);
	}

	#[test]
	fn test_removing_the_reader_clears_its_lookup_views() {
		// A stale entry would keep gating a producer for a flow that no longer exists.
		let mut analyzer = lookup_reader_and_producer();

		analyzer.remove(FlowId(20));

		assert!(analyzer.get_dependency_graph().lookup_views.is_empty());
		assert!(analyzer.get_dependency_graph().dependencies.is_empty());
	}

	fn one_operator_flow(operator: OperatorDef) -> FlowSummary {
		FlowGraphAnalyzer::new().add(create_test_flow_with_nodes(1, vec![operator]))
	}

	fn join_with(with: JoinWith) -> OperatorDef {
		OperatorDef::Join {
			join_type: JoinType::Inner,
			left: vec![],
			right: vec![],
			alias: None,
			natural: false,
			with,
		}
	}

	fn lookup_with(with: LookupWith) -> OperatorDef {
		OperatorDef::Lookup {
			join_type: JoinType::Left,
			right: LookupObject::Table(TableId(9)),
			left: vec![],
			alias: None,
			with,
		}
	}

	fn second() -> Duration {
		Duration::from_seconds(1).unwrap()
	}

	#[test]
	fn every_operator_whose_final_state_ignores_in_between_versions_marks_its_flow_collapsible() {
		// A kind missing here keeps its flows on one graph run per source, which silently loses the collapse
		// gain.
		let column_pick = JoinPick {
			keys: vec![SortKey {
				column: Fragment::internal("tvl"),
				direction: SortDirection::Desc,
			}],
		};
		let allowed = vec![
			(
				"source table",
				SourceTable {
					table: TableId(1),
					time_domain: TimeDomain::None,
				},
			),
			(
				"source view",
				SourceView {
					view: ViewId(2),
				},
			),
			(
				"source ring buffer",
				OperatorDef::SourceRingBuffer {
					ringbuffer: RingBufferId(3),
					time_domain: TimeDomain::None,
				},
			),
			(
				"filter",
				Filter {
					conditions: vec![],
				},
			),
			(
				"map",
				OperatorDef::Map {
					expressions: vec![],
				},
			),
			(
				"extend",
				OperatorDef::Extend {
					expressions: vec![],
				},
			),
			(
				"sink table view",
				SinkTableView {
					view: ViewId(4),
				},
			),
			("hash join", join_with(JoinWith::default())),
			(
				"latest join",
				join_with(JoinWith {
					pick: Some(JoinPick::latest()),
					..JoinWith::default()
				}),
			),
			(
				"earliest join",
				join_with(JoinWith {
					pick: Some(JoinPick::earliest()),
					..JoinWith::default()
				}),
			),
			(
				"column pick join",
				join_with(JoinWith {
					pick: Some(column_pick),
					..JoinWith::default()
				}),
			),
			(
				"join emit after",
				join_with(JoinWith {
					emit: Emit::After(second()),
					..JoinWith::default()
				}),
			),
			(
				"join emit off, not exact",
				join_with(JoinWith {
					emit: Emit::Off,
					pick: Some(JoinPick::latest()),
					exact: false,
					..JoinWith::default()
				}),
			),
			("lookup emit on", lookup_with(LookupWith::default())),
			(
				"lookup emit off, not exact",
				lookup_with(LookupWith {
					emit: Emit::Off,
					exact: false,
					..LookupWith::default()
				}),
			),
			(
				"lookup emit after",
				lookup_with(LookupWith {
					emit: Emit::After(second()),
					..LookupWith::default()
				}),
			),
		];
		for (name, operator) in allowed {
			assert!(one_operator_flow(operator).collapsible_operators, "{name} must allow a collapse");
		}
	}

	#[test]
	fn an_operator_that_needs_every_version_keeps_its_flow_from_collapsing() {
		// One of these collapsed would drop in-between rows or pair a left row with a later right version.
		let retention = || OperatorRetention {
			duration: second(),
		};
		let refused = vec![
			("append", OperatorDef::Append {}),
			(
				"aggregate",
				OperatorDef::Aggregate {
					by: vec![],
					map: vec![],
					with: AggregateWith {},
				},
			),
			(
				"window",
				OperatorDef::Window {
					group_by: vec![],
					aggregations: vec![],
					with: WindowWith {
						kind: WindowKind::Tumbling {
							size: WindowSize::Count(2),
						},
						lateness: None,
						immutable: None,
					},
				},
			),
			(
				"apply",
				OperatorDef::Apply {
					operator: "custom".to_string(),
					params: vec![],
					with: ApplyWith::default(),
				},
			),
			(
				"sink ring buffer view",
				OperatorDef::SinkRingBufferView {
					view: ViewId(5),
					capacity: 4,
				},
			),
			(
				"left join emit on",
				OperatorDef::Join {
					join_type: JoinType::Left,
					left: vec![],
					right: vec![],
					alias: None,
					natural: false,
					with: JoinWith::default(),
				},
			),
			(
				"join emit off, exact",
				join_with(JoinWith {
					emit: Emit::Off,
					pick: Some(JoinPick::latest()),
					exact: true,
					..JoinWith::default()
				}),
			),
			(
				"join with retention",
				join_with(JoinWith {
					retention: Some(JoinRetention {
						left: None,
						right: Some(retention()),
					}),
					..JoinWith::default()
				}),
			),
			(
				"lookup emit off, exact",
				lookup_with(LookupWith {
					emit: Emit::Off,
					exact: true,
					..LookupWith::default()
				}),
			),
			(
				"lookup with retention",
				lookup_with(LookupWith {
					retention: Some(retention()),
					..LookupWith::default()
				}),
			),
		];
		for (name, operator) in refused {
			assert!(!one_operator_flow(operator).collapsible_operators, "{name} must refuse a collapse");
		}
	}

	#[test]
	fn an_emit_off_left_join_still_collapses() {
		// Refusing every left join would undo f3: an emit-off join never re-mints a row when its right arrives.
		let operator = OperatorDef::Join {
			join_type: JoinType::Left,
			left: vec![],
			right: vec![],
			alias: None,
			natural: true,
			with: JoinWith {
				emit: Emit::Off,
				exact: false,
				..JoinWith::default()
			},
		};
		assert!(one_operator_flow(operator).collapsible_operators);
	}

	#[test]
	fn one_refused_operator_keeps_a_flow_of_allowed_ones_from_collapsing() {
		// The flag must hold for every operator of the flow, not the first one or the sink.
		let summary = FlowGraphAnalyzer::new().add(create_test_flow_with_nodes(
			1,
			vec![
				SourceView {
					view: ViewId(2),
				},
				lookup_with(LookupWith {
					emit: Emit::Off,
					exact: true,
					..LookupWith::default()
				}),
				SinkTableView {
					view: ViewId(4),
				},
			],
		));
		assert!(!summary.collapsible_operators);
	}

	#[test]
	fn only_an_exact_lookup_lists_its_reader_as_an_exact_reader_once() {
		// A producer read exactly must keep one layer per source, so a missed exact reader collapses it into
		// drift.
		let mut analyzer = FlowGraphAnalyzer::new();
		let emit_off = || {
			lookup_with(LookupWith {
				emit: Emit::Off,
				exact: true,
				..LookupWith::default()
			})
		};
		let opted_out = || {
			lookup_with(LookupWith {
				emit: Emit::Off,
				exact: false,
				..LookupWith::default()
			})
		};
		let on_view = |operator: OperatorDef| match operator {
			OperatorDef::Lookup {
				join_type,
				left,
				alias,
				with,
				..
			} => OperatorDef::Lookup {
				join_type,
				right: LookupObject::View(ViewId(500)),
				left,
				alias,
				with,
			},
			other => other,
		};
		analyzer.add(create_test_flow_with_nodes(1, vec![on_view(emit_off()), on_view(emit_off())]));
		analyzer.add(create_test_flow_with_nodes(2, vec![on_view(lookup_with(LookupWith::default()))]));
		analyzer.add(create_test_flow_with_nodes(3, vec![emit_off()]));
		analyzer.add(create_test_flow_with_nodes(4, vec![on_view(opted_out())]));

		let graph = analyzer.get_dependency_graph();
		assert_eq!(graph.lookup_views.get(&ViewId(500)), Some(&vec![FlowId(1), FlowId(2), FlowId(4)]));
		assert_eq!(
			graph.exact_lookup_views,
			BTreeMap::from([(ViewId(500), vec![FlowId(1)])]),
			"an emit-on reader, an opted-out reader or a table lookup must never count as an exact view reader"
		);
	}

	#[test]
	fn a_view_feeding_only_a_lookup_right_side_is_not_a_from_read() {
		// A re-emitting lookup's right input is a source node too, and counting it as a from read refuses its
		// producer.
		let mut builder = FlowDag::builder(FlowId(1));
		builder.add_node(FlowNode::new(
			OperatorId(1),
			SourceTable {
				table: TableId(1),
				time_domain: TimeDomain::None,
			},
		));
		builder.add_node(FlowNode::new(
			OperatorId(2),
			SourceView {
				view: ViewId(500),
			},
		));
		builder.add_node(FlowNode::new(OperatorId(3), lookup_with(LookupWith::default())));
		builder.add_node(FlowNode::new(
			OperatorId(4),
			SourceView {
				view: ViewId(501),
			},
		));
		builder.add_node(FlowNode::new(OperatorId(5), OperatorDef::Append {}));
		builder.add_edge(FlowEdge::new(1, FlowId(1), OperatorId(1), OperatorId(3))).unwrap();
		builder.add_edge(FlowEdge::new(2, FlowId(1), OperatorId(2), OperatorId(3))).unwrap();
		builder.add_edge(FlowEdge::new(3, FlowId(1), OperatorId(3), OperatorId(5))).unwrap();
		builder.add_edge(FlowEdge::new(4, FlowId(1), OperatorId(4), OperatorId(5))).unwrap();
		let mut analyzer = FlowGraphAnalyzer::new();
		analyzer.add(builder.build());

		let graph = analyzer.get_dependency_graph();
		assert!(
			graph.source_views.contains_key(&ViewId(500)),
			"precondition: the lookup right side is a source"
		);
		assert_eq!(
			graph.from_views,
			BTreeMap::from([(ViewId(501), vec![FlowId(1)])]),
			"only the appended view is read from, the looked up view is not"
		);
	}
}
