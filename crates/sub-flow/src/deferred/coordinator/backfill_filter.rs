// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections;

use reifydb_core::interface::{
	catalog::{flow::FlowId, id::ViewId, shape::ShapeId},
	cdc::CdcBatch,
	change::{Change, ChangeOrigin},
};
use reifydb_rql::flow::analyzer::FlowDependencyGraph;
use tracing::{Span, field, instrument};

use super::{CoordinatorActor, CoordinatorState};

#[inline]
pub(super) fn collect_chunk_changes(batch: &CdcBatch) -> Vec<Change> {
	let mut chunk_changes = Vec::new();
	for cdc in &batch.items {
		chunk_changes.extend(cdc.changes.iter().cloned());
	}
	chunk_changes
}

#[inline]
pub(super) fn collect_direct_flow_sources(
	dependency_graph: &FlowDependencyGraph,
	flow_id: FlowId,
) -> (collections::HashSet<ShapeId>, Vec<ViewId>) {
	let mut flow_sources: collections::HashSet<ShapeId> = collections::HashSet::new();
	let mut view_sources = Vec::new();

	for (table_id, flow_ids) in &dependency_graph.source_tables {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::Table(*table_id));
		}
	}
	for (view_id, flow_ids) in &dependency_graph.source_views {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::View(*view_id));
			view_sources.push(*view_id);
		}
	}
	for (rb_id, flow_ids) in &dependency_graph.source_ringbuffers {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::RingBuffer(*rb_id));
		}
	}
	for (series_id, flow_ids) in &dependency_graph.source_series {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::Series(*series_id));
		}
	}
	for (dict_id, flow_ids) in &dependency_graph.source_dictionaries {
		if flow_ids.contains(&flow_id) {
			flow_sources.insert(ShapeId::Dictionary(*dict_id));
		}
	}

	(flow_sources, view_sources)
}

#[inline]
pub(super) fn filter_changes_by_sources(
	changes: &[Change],
	flow_sources: &collections::HashSet<ShapeId>,
) -> Vec<Change> {
	changes.iter()
		.filter(|change| {
			if let ChangeOrigin::Shape(source) = change.origin {
				flow_sources.contains(&source)
			} else {
				true
			}
		})
		.cloned()
		.collect()
}

impl CoordinatorActor {
	#[instrument(name = "flow::coordinator::filter_cdc", level = "trace", skip(self, state, changes), fields(
		input = changes.len(),
		output = field::Empty
	))]
	pub(super) fn filter_cdc_for_flow(
		&self,
		state: &CoordinatorState,
		flow_id: FlowId,
		changes: &[Change],
	) -> Vec<Change> {
		let dependency_graph = state.analyzer.get_dependency_graph();
		let (mut flow_sources, view_sources) = collect_direct_flow_sources(dependency_graph, flow_id);
		self.add_transitive_view_sources(dependency_graph, state, &mut flow_sources, view_sources);
		let result = filter_changes_by_sources(changes, &flow_sources);
		Span::current().record("output", result.len());
		result
	}

	#[inline]
	pub(super) fn add_transitive_view_sources(
		&self,
		dependency_graph: &FlowDependencyGraph,
		state: &CoordinatorState,
		flow_sources: &mut collections::HashSet<ShapeId>,
		view_sources: Vec<ViewId>,
	) {
		for view_id in view_sources {
			let Some(producer_flow_id) = dependency_graph.sink_views.get(&view_id) else {
				continue;
			};

			if state.states.contains(producer_flow_id) {
				if let Some(view) = self.catalog.find_view(view_id) {
					flow_sources.insert(view.underlying_id());
				}
				continue;
			}

			for (table_id, flow_ids) in &dependency_graph.source_tables {
				if flow_ids.contains(producer_flow_id) {
					flow_sources.insert(ShapeId::Table(*table_id));
				}
			}
			for (rb_id, flow_ids) in &dependency_graph.source_ringbuffers {
				if flow_ids.contains(producer_flow_id) {
					flow_sources.insert(ShapeId::RingBuffer(*rb_id));
				}
			}
			for (series_id, flow_ids) in &dependency_graph.source_series {
				if flow_ids.contains(producer_flow_id) {
					flow_sources.insert(ShapeId::Series(*series_id));
				}
			}
			for (dict_id, flow_ids) in &dependency_graph.source_dictionaries {
				if flow_ids.contains(producer_flow_id) {
					flow_sources.insert(ShapeId::Dictionary(*dict_id));
				}
			}
		}
	}
}
