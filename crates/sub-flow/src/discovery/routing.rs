// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	iter::once,
};

use reifydb_core::interface::catalog::{flow::FlowId, id::ViewId, object::ObjectId, view::ViewKind};
use reifydb_flow::analyzer::FlowDependencyGraph;
use rustc_hash::{FxHashMap, FxHashSet};

pub fn flow_completeness_objects(
	graph: &FlowDependencyGraph,
	flow: FlowId,
	closure: &BTreeMap<ViewId, BTreeSet<ObjectId>>,
) -> Option<BTreeSet<u64>> {
	let mut admitted: Option<BTreeSet<u64>> = None;
	for (view_id, producer) in &graph.sink_views {
		if *producer != flow {
			continue;
		}
		let objects = closure.get(view_id)?;
		admitted.get_or_insert_with(BTreeSet::new).extend(objects.iter().map(|object| object.to_u64()));
	}
	admitted
}

pub fn flow_upstreams(
	graph: &FlowDependencyGraph,
	flow: FlowId,
	view_kind: &dyn Fn(ViewId) -> Option<ViewKind>,
) -> FxHashMap<FlowId, FxHashSet<ObjectId>> {
	let mut upstreams: FxHashMap<FlowId, FxHashSet<ObjectId>> = FxHashMap::default();
	for (view_id, consumer_flows) in &graph.source_views {
		if !consumer_flows.contains(&flow) {
			continue;
		}
		if view_kind(*view_id) == Some(ViewKind::Transactional) {
			continue;
		}
		let Some(producer) = graph.sink_views.get(view_id) else {
			continue;
		};
		upstreams.entry(*producer).or_default().insert(ObjectId::View(*view_id));
	}
	for (view_id, consumer_flows) in &graph.lookup_views {
		if !consumer_flows.contains(&flow) {
			continue;
		}
		if view_kind(*view_id) == Some(ViewKind::Transactional) {
			continue;
		}
		let Some(producer) = graph.sink_views.get(view_id) else {
			continue;
		};
		if *producer == flow {
			continue;
		}
		upstreams.entry(*producer).or_default();
	}
	upstreams
}

pub fn lookup_producers(graph: &FlowDependencyGraph, flow: FlowId) -> BTreeMap<ViewId, FlowId> {
	graph.lookup_views
		.iter()
		.filter(|(_, consumer_flows)| consumer_flows.contains(&flow))
		.filter_map(|(view_id, _)| graph.sink_views.get(view_id).map(|producer| (*view_id, *producer)))
		.filter(|(_, producer)| *producer != flow)
		.collect()
}

pub fn flow_source_objects(
	graph: &FlowDependencyGraph,
	flow: FlowId,
	registered: &dyn Fn(FlowId) -> bool,
	view_kind: &dyn Fn(ViewId) -> Option<ViewKind>,
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
		if view_kind(*view_id) == Some(ViewKind::Transactional) {
			continue;
		}
		let Some(producer_flow_id) = graph.sink_views.get(view_id) else {
			continue;
		};
		if registered(*producer_flow_id) {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FusedUnit {
	pub head: FlowId,
	pub members: Vec<(FlowId, ViewId)>,
}

impl FusedUnit {
	pub fn flow_ids(&self) -> Vec<FlowId> {
		once(self.head).chain(self.members.iter().map(|(flow, _)| *flow)).collect()
	}
}

pub fn fused_units(
	graph: &FlowDependencyGraph,
	candidates: &BTreeSet<FlowId>,
	view_kind: &dyn Fn(ViewId) -> Option<ViewKind>,
	ringbuffer_sink: &dyn Fn(FlowId) -> bool,
) -> Vec<FusedUnit> {
	let mut parent_of: BTreeMap<FlowId, (FlowId, ViewId)> = BTreeMap::new();
	for child in candidates {
		let Some(view) = sole_view_source(graph, *child) else {
			continue;
		};
		if view_kind(view) != Some(ViewKind::Deferred) {
			continue;
		}
		let Some(parent) = graph.sink_views.get(&view).copied() else {
			continue;
		};
		if parent == *child || !candidates.contains(&parent) || ringbuffer_sink(parent) {
			continue;
		}
		if graph.source_views.get(&view).is_none_or(|consumers| consumers.as_slice() != [*child]) {
			continue;
		}
		parent_of.insert(*child, (parent, view));
	}
	let child_of: BTreeMap<FlowId, (FlowId, ViewId)> =
		parent_of.iter().map(|(child, (parent, view))| (*parent, (*child, *view))).collect();

	let mut units = Vec::new();
	for head in candidates {
		if parent_of.contains_key(head) || !child_of.contains_key(head) {
			continue;
		}
		let mut unit = FusedUnit {
			head: *head,
			members: Vec::new(),
		};
		let mut produced: BTreeSet<ViewId> = produced_by(graph, *head);
		let mut tail = *head;
		while let Some((child, view)) = child_of.get(&tail).copied() {
			let looks_up_inside = graph.lookup_views.iter().any(|(looked_up, consumers)| {
				consumers.contains(&child) && produced.contains(looked_up)
			});
			if looks_up_inside {
				break;
			}
			unit.members.push((child, view));
			produced.extend(produced_by(graph, child));
			tail = child;
		}
		if !unit.members.is_empty() {
			units.push(unit);
		}
	}
	units
}

pub fn unit_upstreams(
	graph: &FlowDependencyGraph,
	unit: &FusedUnit,
	view_kind: &dyn Fn(ViewId) -> Option<ViewKind>,
) -> FxHashMap<FlowId, FxHashSet<ObjectId>> {
	let inside: BTreeSet<FlowId> = unit.flow_ids().into_iter().collect();
	let mut upstreams: FxHashMap<FlowId, FxHashSet<ObjectId>> = FxHashMap::default();
	for member in &inside {
		for (producer, objects) in flow_upstreams(graph, *member, view_kind) {
			if inside.contains(&producer) {
				continue;
			}
			upstreams.entry(producer).or_default().extend(objects);
		}
	}
	upstreams
}

pub fn unit_lookup_producers(graph: &FlowDependencyGraph, unit: &FusedUnit) -> BTreeMap<ViewId, FlowId> {
	let mut producers = BTreeMap::new();
	for member in unit.flow_ids() {
		producers.extend(lookup_producers(graph, member));
	}
	producers
}

fn produced_by(graph: &FlowDependencyGraph, flow: FlowId) -> BTreeSet<ViewId> {
	graph.sink_views.iter().filter(|(_, producer)| **producer == flow).map(|(view, _)| *view).collect()
}

fn sole_view_source(graph: &FlowDependencyGraph, flow: FlowId) -> Option<ViewId> {
	let tables = graph.source_tables.values().filter(|flows| flows.contains(&flow)).count();
	let ringbuffers = graph.source_ringbuffers.values().filter(|flows| flows.contains(&flow)).count();
	let series = graph.source_series.values().filter(|flows| flows.contains(&flow)).count();
	let views: Vec<ViewId> =
		graph.source_views.iter().filter(|(_, flows)| flows.contains(&flow)).map(|(view, _)| *view).collect();
	match (tables + ringbuffers + series, views.as_slice()) {
		(0, [view]) => Some(*view),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::id::{RingBufferId, SeriesId, TableId};

	use super::*;
	use crate::progress::tracker::FlowPositionTracker;

	fn empty_graph() -> FlowDependencyGraph {
		FlowDependencyGraph {
			flows: Vec::new(),
			dependencies: Vec::new(),
			source_tables: BTreeMap::new(),
			source_views: BTreeMap::new(),
			source_ringbuffers: BTreeMap::new(),
			source_series: BTreeMap::new(),
			sink_views: BTreeMap::new(),
			lookup_views: BTreeMap::new(),
		}
	}

	fn no_views(_view_id: ViewId) -> Option<ViewKind> {
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
	fn registered_producer_routes_the_view_and_nothing_else() {
		// The reader wakes on the view id alone; a producer source would wake it before the view is written.
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));
		graph.source_tables.insert(TableId(1), vec![FlowId(10)]);

		let registered = |f: FlowId| f == FlowId(10);
		let view_kind = |view_id: ViewId| {
			assert_eq!(view_id, ViewId(5));
			Some(ViewKind::Deferred)
		};

		let objects = flow_source_objects(&graph, FlowId(20), &registered, &view_kind);

		assert_eq!(objects.into_iter().collect::<Vec<_>>(), vec![ObjectId::View(ViewId(5))]);
	}

	#[test]
	fn transactional_producer_view_adds_no_indirection() {
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));
		graph.source_tables.insert(TableId(1), vec![FlowId(10)]);

		let registered = |f: FlowId| f == FlowId(10);
		let view_kind = |_view_id: ViewId| Some(ViewKind::Transactional);

		let objects = flow_source_objects(&graph, FlowId(20), &registered, &view_kind);

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

		let view_kind = |_view_id: ViewId| Some(ViewKind::Deferred);

		let objects = flow_source_objects(&graph, FlowId(20), &none_registered, &view_kind);

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

		let view_kind = |_view_id: ViewId| Some(ViewKind::Deferred);

		let objects = flow_source_objects(&graph, FlowId(20), &none_registered, &view_kind);

		assert_eq!(objects.into_iter().collect::<Vec<_>>(), vec![ObjectId::View(ViewId(5))]);
	}

	#[test]
	fn a_deferred_view_gates_on_its_producer_whether_or_not_it_is_registered() {
		// A reader that skipped the gate while its producer is not yet spawned would run ahead of the view
		// rows.
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.source_views.insert(ViewId(6), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));
		graph.sink_views.insert(ViewId(6), FlowId(10));
		graph.source_tables.insert(TableId(1), vec![FlowId(20)]);

		let view_kind = |_view_id: ViewId| Some(ViewKind::Deferred);

		assert_eq!(
			flow_upstreams(&graph, FlowId(20), &view_kind),
			FxHashMap::from_iter([(
				FlowId(10),
				FxHashSet::from_iter([ObjectId::View(ViewId(5)), ObjectId::View(ViewId(6))])
			)]),
			"both views of one producer must share one stream, or a commit that writes both is read twice"
		);
	}

	#[test]
	fn transactional_and_producerless_views_carry_no_gate() {
		// Their rows commit at their own source version, so a gate would wait on a position nobody publishes.
		let mut graph = empty_graph();
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.source_views.insert(ViewId(6), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));

		let view_kind = |view_id: ViewId| {
			if view_id == ViewId(5) {
				Some(ViewKind::Transactional)
			} else {
				Some(ViewKind::Deferred)
			}
		};

		assert!(flow_upstreams(&graph, FlowId(20), &view_kind).is_empty());
	}

	#[test]
	fn the_admission_set_unions_every_sink_view_the_flow_owns() {
		// Intersecting, or taking only the first sink, would leave the other sink's ancestors unable to wake
		// the flow.
		let mut graph = empty_graph();
		graph.sink_views.insert(ViewId(5), FlowId(10));
		graph.sink_views.insert(ViewId(6), FlowId(10));
		graph.sink_views.insert(ViewId(7), FlowId(99));
		let closure = BTreeMap::from([
			(ViewId(5), BTreeSet::from([ObjectId::Table(TableId(1))])),
			(ViewId(6), BTreeSet::from([ObjectId::Table(TableId(2))])),
			(ViewId(7), BTreeSet::from([ObjectId::Table(TableId(3))])),
		]);

		let admitted = flow_completeness_objects(&graph, FlowId(10), &closure);

		assert_eq!(admitted, Some(BTreeSet::from([1, 2])), "another flow's closure must not leak in");
	}

	#[test]
	fn a_sink_view_missing_from_the_closure_admits_everything() {
		// Filtering on a partial closure would silently stop waking the flow, so a missing sink must forfeit
		// the filter.
		let mut graph = empty_graph();
		graph.sink_views.insert(ViewId(5), FlowId(10));
		graph.sink_views.insert(ViewId(6), FlowId(10));
		let closure = BTreeMap::from([(ViewId(5), BTreeSet::from([ObjectId::Table(TableId(1))]))]);

		assert_eq!(
			flow_completeness_objects(&graph, FlowId(10), &closure),
			None,
			"one missing sink must forfeit the filter for the whole flow, not just for that sink"
		);
	}

	#[test]
	fn a_flow_that_sinks_nowhere_near_a_view_admits_everything() {
		// The closure is keyed by sink view, so a flow sinking to a table or ring buffer must keep its
		// unconditional wake.
		let mut graph = empty_graph();
		graph.source_tables.insert(TableId(1), vec![FlowId(10)]);

		assert_eq!(flow_completeness_objects(&graph, FlowId(10), &BTreeMap::new()), None);
	}

	#[test]
	fn the_admission_set_is_kind_agnostic_because_an_assertion_carries_only_a_raw_id() {
		// A set holding ObjectId would never match a series asserting the same numeric id as a table.
		let mut graph = empty_graph();
		graph.sink_views.insert(ViewId(5), FlowId(10));
		let closure = BTreeMap::from([(ViewId(5), BTreeSet::from([ObjectId::Series(SeriesId(9))]))]);

		assert_eq!(flow_completeness_objects(&graph, FlowId(10), &closure), Some(BTreeSet::from([9])));
	}

	fn chain() -> FlowDependencyGraph {
		let mut graph = empty_graph();
		graph.source_tables.insert(TableId(1), vec![FlowId(10)]);
		graph.sink_views.insert(ViewId(5), FlowId(10));
		graph.source_views.insert(ViewId(5), vec![FlowId(20)]);
		graph.sink_views.insert(ViewId(6), FlowId(20));
		graph.source_views.insert(ViewId(6), vec![FlowId(30)]);
		graph.sink_views.insert(ViewId(7), FlowId(30));
		graph
	}

	fn all_deferred(_view_id: ViewId) -> Option<ViewKind> {
		Some(ViewKind::Deferred)
	}

	fn no_ringbuffer(_flow_id: FlowId) -> bool {
		false
	}

	fn all_of(graph: &FlowDependencyGraph) -> BTreeSet<FlowId> {
		graph.sink_views.values().copied().collect()
	}

	#[test]
	fn a_single_parent_chain_of_deferred_views_forms_one_unit_writer_first() {
		// A unit out of writer order would feed a child before its parent produced the rows it reads.
		let graph = chain();
		let units = fused_units(&graph, &all_of(&graph), &all_deferred, &no_ringbuffer);
		assert_eq!(
			units,
			vec![FusedUnit {
				head: FlowId(10),
				members: vec![(FlowId(20), ViewId(5)), (FlowId(30), ViewId(6))],
			}]
		);
	}

	#[test]
	fn a_fused_child_is_not_routed_its_parents_change_stream() {
		// A fused child still routed to its parent's view would apply every parent row twice.
		let mut graph = chain();
		graph.source_tables.insert(TableId(9), vec![FlowId(40)]);
		graph.sink_views.insert(ViewId(8), FlowId(40));
		graph.lookup_views.insert(ViewId(8), vec![FlowId(30)]);
		let unit = fused_units(&graph, &all_of(&graph), &all_deferred, &no_ringbuffer).remove(0);
		let upstreams = unit_upstreams(&graph, &unit, &all_deferred);
		assert!(!upstreams.contains_key(&FlowId(10)), "an in-unit parent stays an upstream");
		assert!(!upstreams.contains_key(&FlowId(20)), "an in-unit parent stays an upstream");
		assert!(upstreams.contains_key(&FlowId(40)), "an outside lookup producer must still gate the unit");
		assert_eq!(unit_lookup_producers(&graph, &unit), BTreeMap::from([(ViewId(8), FlowId(40))]));
	}

	#[test]
	fn a_unit_has_readers_only_through_a_flow_outside_it() {
		// A member counted as its parent's reader makes the unit commit once per source version for nobody.
		let mut graph = chain();
		graph.source_views.insert(ViewId(7), vec![FlowId(40)]);
		graph.source_tables.insert(TableId(9), vec![FlowId(40)]);
		graph.sink_views.insert(ViewId(8), FlowId(40));
		let units = fused_units(&graph, &all_of(&graph), &all_deferred, &no_ringbuffer);
		assert_eq!(units.len(), 1, "the two-source reader must stay outside the unit: {units:?}");
		let unit = &units[0];
		let tracker = FlowPositionTracker::new();
		tracker.set_upstreams(unit.head, unit_upstreams(&graph, unit, &all_deferred));
		for (member, _) in &unit.members {
			tracker.set_upstreams(*member, Default::default());
		}
		tracker.set_upstreams(FlowId(40), flow_upstreams(&graph, FlowId(40), &all_deferred));
		assert!(!tracker.has_readers(FlowId(10)), "the head is read only by its own member");
		assert!(!tracker.has_readers(FlowId(20)), "the middle member is read only by its own member");
		assert!(tracker.has_readers(FlowId(30)), "the tail is read by the flow outside the unit");
	}

	#[test]
	fn unit_formation_refuses_a_lookup_of_an_in_unit_producer() {
		// A lookup reads committed state only, so a member looking up its own unit would read the previous
		// step.
		let mut graph = chain();
		graph.lookup_views.insert(ViewId(5), vec![FlowId(30)]);
		let units = fused_units(&graph, &all_of(&graph), &all_deferred, &no_ringbuffer);
		assert_eq!(
			units,
			vec![FusedUnit {
				head: FlowId(10),
				members: vec![(FlowId(20), ViewId(5))],
			}]
		);
	}

	#[test]
	fn unit_formation_refuses_a_ring_buffer_parent() {
		// A ring buffer parent evicts without emitting, so a fused child would keep rows the parent dropped.
		let graph = chain();
		let ring = |flow: FlowId| flow == FlowId(10);
		let units = fused_units(&graph, &all_of(&graph), &all_deferred, &ring);
		assert_eq!(
			units,
			vec![FusedUnit {
				head: FlowId(20),
				members: vec![(FlowId(30), ViewId(6))],
			}]
		);
	}

	#[test]
	fn unit_formation_refuses_a_second_child() {
		// A parent with two children has a reader outside any chain, so neither edge may fuse.
		let mut graph = chain();
		graph.source_views.insert(ViewId(5), vec![FlowId(20), FlowId(50)]);
		graph.sink_views.insert(ViewId(9), FlowId(50));
		let units = fused_units(&graph, &all_of(&graph), &all_deferred, &no_ringbuffer);
		assert_eq!(
			units,
			vec![FusedUnit {
				head: FlowId(20),
				members: vec![(FlowId(30), ViewId(6))],
			}]
		);
	}

	#[test]
	fn unit_formation_refuses_a_transactional_view_edge() {
		// A transactional view is fused by flow-sync already, so a deferred unit must not claim it.
		let graph = chain();
		let kind = |view: ViewId| {
			Some(if view == ViewId(5) {
				ViewKind::Transactional
			} else {
				ViewKind::Deferred
			})
		};
		let units = fused_units(&graph, &all_of(&graph), &kind, &no_ringbuffer);
		assert_eq!(
			units,
			vec![FusedUnit {
				head: FlowId(20),
				members: vec![(FlowId(30), ViewId(6))],
			}]
		);
	}
}

#[cfg(test)]
mod lookup_tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::{flow::FlowId, id::ViewId, object::ObjectId, view::ViewKind};
	use reifydb_flow::analyzer::FlowDependencyGraph;

	use super::{flow_upstreams, lookup_producers};

	const PRODUCER: FlowId = FlowId(1);
	const READER: FlowId = FlowId(2);
	const PRICES: ViewId = ViewId(10);
	const LEVELS: ViewId = ViewId(11);

	fn graph(
		lookup_views: BTreeMap<ViewId, Vec<FlowId>>,
		source_views: BTreeMap<ViewId, Vec<FlowId>>,
	) -> FlowDependencyGraph {
		FlowDependencyGraph {
			flows: Vec::new(),
			dependencies: Vec::new(),
			source_tables: BTreeMap::new(),
			source_views,
			source_ringbuffers: BTreeMap::new(),
			source_series: BTreeMap::new(),
			sink_views: BTreeMap::from([(PRICES, PRODUCER), (LEVELS, PRODUCER)]),
			lookup_views,
		}
	}

	fn deferred(_: ViewId) -> Option<ViewKind> {
		Some(ViewKind::Deferred)
	}

	#[test]
	fn a_looked_up_view_gates_on_its_producer_with_no_view_to_read() {
		// MD32: the producer must count for the gate, but an empty set keeps its CDC out of the merge.
		let g = graph(BTreeMap::from([(PRICES, vec![READER])]), BTreeMap::new());
		let upstreams = flow_upstreams(&g, READER, &deferred);

		assert_eq!(upstreams.len(), 1);
		assert!(upstreams.get(&PRODUCER).is_some_and(|views| views.is_empty()), "the view must not be read");
		assert_eq!(lookup_producers(&g, READER), BTreeMap::from([(PRICES, PRODUCER)]));
		assert!(lookup_producers(&g, PRODUCER).is_empty(), "the producer looks nothing up");
	}

	#[test]
	fn a_producer_also_read_as_a_source_keeps_its_source_view() {
		// A gate-only entry must never erase a real source view of the same producer.
		let g = graph(BTreeMap::from([(PRICES, vec![READER])]), BTreeMap::from([(LEVELS, vec![READER])]));
		let upstreams = flow_upstreams(&g, READER, &deferred);

		let views = upstreams.get(&PRODUCER).expect("the producer is upstream");
		assert_eq!(views.len(), 1);
		assert!(views.contains(&ObjectId::View(LEVELS)));
	}

	#[test]
	fn a_transactional_looked_up_view_has_no_gate() {
		// A transactional view has no producer flow to wait for; it is read like a table.
		let g = graph(BTreeMap::from([(PRICES, vec![READER])]), BTreeMap::new());
		let upstreams = flow_upstreams(&g, READER, &|_| Some(ViewKind::Transactional));

		assert!(upstreams.is_empty());
	}

	#[test]
	fn a_flow_looking_up_its_own_view_is_not_gated_on_itself() {
		// Gating a flow on its own completion would stall it forever.
		let g = graph(BTreeMap::from([(PRICES, vec![PRODUCER])]), BTreeMap::new());

		assert!(flow_upstreams(&g, PRODUCER, &deferred).is_empty());
		assert!(lookup_producers(&g, PRODUCER).is_empty());
	}
}
