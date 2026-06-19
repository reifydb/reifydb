// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections, collections::BTreeMap, sync::Arc};

use reifydb_core::{
	actors::flow::{FlowCoordinatorMessage, FlowPoolMessage, PoolResponse},
	interface::catalog::{flow::FlowId, shape::ShapeId, view::ViewKind},
};
use reifydb_runtime::actor::context::Context;

use super::{ConsumeContext, CoordinatorActor, CoordinatorState, Phase, coordinator_error};

impl CoordinatorActor {
	#[inline]
	pub(super) fn continue_rebalancing(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		response: PoolResponse,
		consume_ctx: ConsumeContext,
	) {
		match response {
			PoolResponse::Success {
				..
			}
			| PoolResponse::RegisterSuccess => {
				self.proceed_to_submit(state, ctx, consume_ctx);
			}
			PoolResponse::Error(e) => {
				(consume_ctx.original_reply)(coordinator_error(e));
			}
		}
	}

	pub(super) fn rebuild_routing_cache(&self, state: &mut CoordinatorState) {
		let active_vec = state.states.active_flow_ids();
		let active: collections::HashSet<FlowId> = active_vec.iter().copied().collect();
		let index = self.build_routing_index(state, &active);
		state.cached_active = Arc::new(active_vec);
		state.cached_routing_index = Arc::new(index);
	}

	fn build_routing_index(
		&self,
		state: &CoordinatorState,
		active: &collections::HashSet<FlowId>,
	) -> collections::HashMap<ShapeId, Vec<FlowId>> {
		let g = state.analyzer.get_dependency_graph();
		let mut index: collections::HashMap<ShapeId, Vec<FlowId>> = collections::HashMap::new();

		let add = |index: &mut collections::HashMap<ShapeId, Vec<FlowId>>, shape: ShapeId, flows: &[FlowId]| {
			for f in flows {
				if active.contains(f) {
					index.entry(shape).or_default().push(*f);
				}
			}
		};

		for (table_id, flows) in &g.source_tables {
			add(&mut index, ShapeId::Table(*table_id), flows);
		}
		for (view_id, flows) in &g.source_views {
			add(&mut index, ShapeId::View(*view_id), flows);
		}
		for (rb_id, flows) in &g.source_ringbuffers {
			add(&mut index, ShapeId::RingBuffer(*rb_id), flows);
		}
		for (series_id, flows) in &g.source_series {
			add(&mut index, ShapeId::Series(*series_id), flows);
		}
		for (dict_id, flows) in &g.source_dictionaries {
			add(&mut index, ShapeId::Dictionary(*dict_id), flows);
		}

		for (view_id, consumer_flows) in &g.source_views {
			let active_consumers: Vec<FlowId> =
				consumer_flows.iter().copied().filter(|f| active.contains(f)).collect();
			if active_consumers.is_empty() {
				continue;
			}
			if self.catalog.find_view(*view_id).map(|v| v.kind()) == Some(ViewKind::Transactional) {
				continue;
			}
			let Some(producer_flow_id) = g.sink_views.get(view_id) else {
				continue;
			};
			if state.states.contains(producer_flow_id) {
				if let Some(view) = self.catalog.find_view(*view_id) {
					add(&mut index, view.underlying_id(), &active_consumers);
				}
				continue;
			}
			for (table_id, flow_ids) in &g.source_tables {
				if flow_ids.contains(producer_flow_id) {
					add(&mut index, ShapeId::Table(*table_id), &active_consumers);
				}
			}
			for (rb_id, flow_ids) in &g.source_ringbuffers {
				if flow_ids.contains(producer_flow_id) {
					add(&mut index, ShapeId::RingBuffer(*rb_id), &active_consumers);
				}
			}
			for (series_id, flow_ids) in &g.source_series {
				if flow_ids.contains(producer_flow_id) {
					add(&mut index, ShapeId::Series(*series_id), &active_consumers);
				}
			}
			for (dict_id, flow_ids) in &g.source_dictionaries {
				if flow_ids.contains(producer_flow_id) {
					add(&mut index, ShapeId::Dictionary(*dict_id), &active_consumers);
				}
			}
		}

		for flows in index.values_mut() {
			flows.sort_unstable_by_key(|f| f.0);
			flows.dedup();
		}
		index
	}

	pub(super) fn compute_flow_assignments(&self, state: &CoordinatorState) -> BTreeMap<FlowId, usize> {
		let dependency_graph = state.analyzer.get_dependency_graph();

		let mut upstream_of: BTreeMap<FlowId, FlowId> = BTreeMap::new();
		for dep in &dependency_graph.dependencies {
			upstream_of.entry(dep.target_flow).or_insert(dep.source_flow);
		}

		let mut assignments: BTreeMap<FlowId, usize> = BTreeMap::new();
		let levels = state.analyzer.calculate_execution_levels(dependency_graph);
		for level in &levels {
			for fid in level {
				let worker_id = match upstream_of.get(fid) {
					Some(upstream) => assignments
						.get(upstream)
						.copied()
						.unwrap_or_else(|| (upstream.0 as usize) % self.num_workers),
					None => (fid.0 as usize) % self.num_workers,
				};
				assignments.insert(*fid, worker_id);
			}
		}
		assignments
	}

	pub(super) fn rebalance_flows(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		consume_ctx: ConsumeContext,
	) {
		let assignments = self.compute_flow_assignments(state);
		state.flow_assignments = assignments.clone();

		let mut by_worker: BTreeMap<usize, Vec<FlowId>> = BTreeMap::new();
		for (fid, wid) in &assignments {
			by_worker.entry(*wid).or_default().push(*fid);
		}

		let self_ref = ctx.self_ref().clone();
		let callback: Box<dyn FnOnce(PoolResponse) + Send> = Box::new(move |resp| {
			let _ = self_ref.send(FlowCoordinatorMessage::PoolReply(resp));
		});

		if self.pool
			.send(FlowPoolMessage::Rebalance {
				assignments: by_worker,
				reply: callback,
			})
			.is_err()
		{
			(consume_ctx.original_reply)(coordinator_error("Pool actor stopped"));
			return;
		}

		state.set_phase(
			Phase::Rebalancing {
				ctx: consume_ctx,
			},
			self.clock.instant(),
		);
	}
}
