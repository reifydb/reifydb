// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_core::interface::catalog::{
	flow::FlowId, id::ViewId, object::ObjectId, storage::StorageId, view::ViewKind,
};
use reifydb_rql::flow::analyzer::FlowDependencyGraph;

pub struct ViewRoute {
	pub kind: ViewKind,
	pub storage: StorageId,
}

pub fn flow_source_objects(
	graph: &FlowDependencyGraph,
	flow: FlowId,
	registered: &dyn Fn(FlowId) -> bool,
	view_route: &dyn Fn(ViewId) -> Option<ViewRoute>,
) -> BTreeSet<ObjectId> {
	let mut objects = BTreeSet::new();

	for (table_id, flows) in &graph.source_tables {
		if flows.contains(&flow) {
			objects.insert(ObjectId::Table(*table_id));
		}
	}
	for (view_id, flows) in &graph.source_views {
		if flows.contains(&flow) {
			objects.insert(ObjectId::View(*view_id));
		}
	}
	for (rb_id, flows) in &graph.source_ringbuffers {
		if flows.contains(&flow) {
			objects.insert(ObjectId::RingBuffer(*rb_id));
		}
	}
	for (series_id, flows) in &graph.source_series {
		if flows.contains(&flow) {
			objects.insert(ObjectId::Series(*series_id));
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
				objects.insert(route.storage.into());
			}
			continue;
		}
		for (table_id, flow_ids) in &graph.source_tables {
			if flow_ids.contains(producer_flow_id) {
				objects.insert(ObjectId::Table(*table_id));
			}
		}
		for (rb_id, flow_ids) in &graph.source_ringbuffers {
			if flow_ids.contains(producer_flow_id) {
				objects.insert(ObjectId::RingBuffer(*rb_id));
			}
		}
		for (series_id, flow_ids) in &graph.source_series {
			if flow_ids.contains(producer_flow_id) {
				objects.insert(ObjectId::Series(*series_id));
			}
		}
	}

	objects
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::id::{RingBufferId, TableId};

	use super::*;

	fn empty_graph() -> FlowDependencyGraph {
		FlowDependencyGraph {
			flows: Vec::new(),
			dependencies: Vec::new(),
			source_tables: BTreeMap::new(),
			source_views: BTreeMap::new(),
			source_ringbuffers: BTreeMap::new(),
			source_series: BTreeMap::new(),
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
		graph.source_tables.insert(TableId(4), vec![FlowId(99)]);

		let objects = flow_source_objects(&graph, FlowId(10), &none_registered, &no_views);

		assert_eq!(
			objects.into_iter().collect::<Vec<_>>(),
			vec![ObjectId::Table(TableId(1)), ObjectId::RingBuffer(RingBufferId(2))]
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
				storage: StorageId::Table(TableId(500)),
			})
		};

		let objects = flow_source_objects(&graph, FlowId(20), &registered, &view_route);

		assert_eq!(
			objects.into_iter().collect::<Vec<_>>(),
			vec![ObjectId::Table(TableId(500)), ObjectId::View(ViewId(5))]
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
				storage: StorageId::Table(TableId(500)),
			})
		};

		let objects = flow_source_objects(&graph, FlowId(20), &registered, &view_route);

		assert_eq!(objects.into_iter().collect::<Vec<_>>(), vec![ObjectId::View(ViewId(5))]);
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
				storage: StorageId::Table(TableId(500)),
			})
		};

		let objects = flow_source_objects(&graph, FlowId(20), &none_registered, &view_route);

		assert_eq!(
			objects.into_iter().collect::<Vec<_>>(),
			vec![
				ObjectId::Table(TableId(1)),
				ObjectId::View(ViewId(5)),
				ObjectId::RingBuffer(RingBufferId(2)),
			]
		);
	}

	#[test]
	fn view_missing_from_catalog_routes_view_only() {
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));

		let registered = |f: FlowId| f == FlowId(10);

		let objects = flow_source_objects(&graph, FlowId(20), &registered, &no_views);

		assert_eq!(objects.into_iter().collect::<Vec<_>>(), vec![ObjectId::View(ViewId(5))]);
	}

	#[test]
	fn view_without_producer_routes_view_only() {
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);

		let view_route = |_view_id: ViewId| {
			Some(ViewRoute {
				kind: ViewKind::Deferred,
				storage: StorageId::Table(TableId(500)),
			})
		};

		let objects = flow_source_objects(&graph, FlowId(20), &none_registered, &view_route);

		assert_eq!(objects.into_iter().collect::<Vec<_>>(), vec![ObjectId::View(ViewId(5))]);
	}
}
