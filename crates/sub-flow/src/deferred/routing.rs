// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_core::interface::catalog::{flow::FlowId, id::ViewId, shape::ShapeId, view::ViewKind};
use reifydb_rql::flow::analyzer::FlowDependencyGraph;

pub struct ViewRoute {
	pub kind: ViewKind,
	pub underlying: ShapeId,
}

pub fn flow_source_shapes(
	graph: &FlowDependencyGraph,
	flow: FlowId,
	registered: &dyn Fn(FlowId) -> bool,
	view_route: &dyn Fn(ViewId) -> Option<ViewRoute>,
) -> BTreeSet<ShapeId> {
	let mut shapes = BTreeSet::new();

	for (table_id, flows) in &graph.source_tables {
		if flows.contains(&flow) {
			shapes.insert(ShapeId::Table(*table_id));
		}
	}
	for (view_id, flows) in &graph.source_views {
		if flows.contains(&flow) {
			shapes.insert(ShapeId::View(*view_id));
		}
	}
	for (rb_id, flows) in &graph.source_ringbuffers {
		if flows.contains(&flow) {
			shapes.insert(ShapeId::RingBuffer(*rb_id));
		}
	}
	for (series_id, flows) in &graph.source_series {
		if flows.contains(&flow) {
			shapes.insert(ShapeId::Series(*series_id));
		}
	}
	for (dict_id, flows) in &graph.source_dictionaries {
		if flows.contains(&flow) {
			shapes.insert(ShapeId::Dictionary(*dict_id));
		}
	}

	for (view_id, consumer_flows) in &graph.source_views {
		if !consumer_flows.contains(&flow) {
			continue;
		}
		let route = view_route(*view_id);
		if matches!(&route, Some(r) if r.kind == ViewKind::Transactional) {
			continue;
		}
		let Some(producer_flow_id) = graph.sink_views.get(view_id) else {
			continue;
		};
		if registered(*producer_flow_id) {
			if let Some(route) = route {
				shapes.insert(route.underlying);
			}
			continue;
		}
		for (table_id, flow_ids) in &graph.source_tables {
			if flow_ids.contains(producer_flow_id) {
				shapes.insert(ShapeId::Table(*table_id));
			}
		}
		for (rb_id, flow_ids) in &graph.source_ringbuffers {
			if flow_ids.contains(producer_flow_id) {
				shapes.insert(ShapeId::RingBuffer(*rb_id));
			}
		}
		for (series_id, flow_ids) in &graph.source_series {
			if flow_ids.contains(producer_flow_id) {
				shapes.insert(ShapeId::Series(*series_id));
			}
		}
		for (dict_id, flow_ids) in &graph.source_dictionaries {
			if flow_ids.contains(producer_flow_id) {
				shapes.insert(ShapeId::Dictionary(*dict_id));
			}
		}
	}

	shapes
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::id::{RingBufferId, TableId};
	use reifydb_value::value::dictionary::DictionaryId;

	use super::*;

	fn empty_graph() -> FlowDependencyGraph {
		FlowDependencyGraph {
			flows: Vec::new(),
			dependencies: Vec::new(),
			source_tables: BTreeMap::new(),
			source_views: BTreeMap::new(),
			source_ringbuffers: BTreeMap::new(),
			source_series: BTreeMap::new(),
			source_dictionaries: BTreeMap::new(),
			sink_views: BTreeMap::new(),
		}
	}

	fn no_views(_view_id: ViewId) -> Option<ViewRoute> {
		None
	}

	fn none_registered(_flow_id: FlowId) -> bool {
		false
	}

	#[test]
	fn direct_sources_route_one_to_one() {
		let mut graph = empty_graph();
		graph.source_tables.insert(TableId(1), vec![FlowId(10)]);
		graph.source_ringbuffers.insert(RingBufferId(2), vec![FlowId(10)]);
		graph.source_dictionaries.insert(DictionaryId(3), vec![FlowId(10)]);
		graph.source_tables.insert(TableId(4), vec![FlowId(99)]);

		let shapes = flow_source_shapes(&graph, FlowId(10), &none_registered, &no_views);

		assert_eq!(
			shapes.into_iter().collect::<Vec<_>>(),
			vec![
				ShapeId::Table(TableId(1)),
				ShapeId::RingBuffer(RingBufferId(2)),
				ShapeId::Dictionary(DictionaryId(3)),
			]
		);
	}

	#[test]
	fn registered_producer_routes_view_underlying() {
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));

		let registered = |f: FlowId| f == FlowId(10);
		let view_route = |view_id: ViewId| {
			assert_eq!(view_id, ViewId(5));
			Some(ViewRoute {
				kind: ViewKind::Deferred,
				underlying: ShapeId::Table(TableId(500)),
			})
		};

		let shapes = flow_source_shapes(&graph, FlowId(20), &registered, &view_route);

		assert_eq!(
			shapes.into_iter().collect::<Vec<_>>(),
			vec![ShapeId::Table(TableId(500)), ShapeId::View(ViewId(5))]
		);
	}

	#[test]
	fn transactional_producer_view_adds_no_indirection() {
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));
		graph.source_tables.insert(TableId(1), vec![FlowId(10)]);

		let registered = |f: FlowId| f == FlowId(10);
		let view_route = |_view_id: ViewId| {
			Some(ViewRoute {
				kind: ViewKind::Transactional,
				underlying: ShapeId::Table(TableId(500)),
			})
		};

		let shapes = flow_source_shapes(&graph, FlowId(20), &registered, &view_route);

		assert_eq!(shapes.into_iter().collect::<Vec<_>>(), vec![ShapeId::View(ViewId(5))]);
	}

	#[test]
	fn unregistered_producer_routes_its_direct_sources() {
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));
		graph.source_tables.insert(TableId(1), vec![FlowId(10)]);
		graph.source_ringbuffers.insert(RingBufferId(2), vec![FlowId(10)]);
		graph.source_tables.insert(TableId(7), vec![FlowId(99)]);

		let view_route = |_view_id: ViewId| {
			Some(ViewRoute {
				kind: ViewKind::Deferred,
				underlying: ShapeId::Table(TableId(500)),
			})
		};

		let shapes = flow_source_shapes(&graph, FlowId(20), &none_registered, &view_route);

		assert_eq!(
			shapes.into_iter().collect::<Vec<_>>(),
			vec![
				ShapeId::Table(TableId(1)),
				ShapeId::View(ViewId(5)),
				ShapeId::RingBuffer(RingBufferId(2)),
			]
		);
	}

	#[test]
	fn view_missing_from_catalog_routes_view_only() {
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));

		let registered = |f: FlowId| f == FlowId(10);

		let shapes = flow_source_shapes(&graph, FlowId(20), &registered, &no_views);

		assert_eq!(shapes.into_iter().collect::<Vec<_>>(), vec![ShapeId::View(ViewId(5))]);
	}

	#[test]
	fn view_without_producer_routes_view_only() {
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);

		let view_route = |_view_id: ViewId| {
			Some(ViewRoute {
				kind: ViewKind::Deferred,
				underlying: ShapeId::Table(TableId(500)),
			})
		};

		let shapes = flow_source_shapes(&graph, FlowId(20), &none_registered, &view_route);

		assert_eq!(shapes.into_iter().collect::<Vec<_>>(), vec![ShapeId::View(ViewId(5))]);
	}
}
