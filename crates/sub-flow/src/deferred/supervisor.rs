// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
};

use reifydb_cdc::consume::{
	backlog::{BacklogPull, FlowBacklog},
	checkpoint::CdcCheckpoint,
	watermark::CdcConsumerWatermark,
};
use reifydb_core::{
	actors::flow::{FlowActorHandle, FlowActorMessage, FlowSupervisorMessage},
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, object::ObjectId, view::ViewKind},
		cdc::{Cdc, CdcConsumerId},
		change::ChangeOrigin,
	},
	lifecycle::metrics::RetentionMetrics,
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_engine::{engine::StandardEngine, vm::flow_lineage::ViewLineage};
use reifydb_flow::transaction::substrate::FlowSubstrate;
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, flow::FlowDag, operator::OperatorDef};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorSpawner},
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::{debug, error, warn};

use crate::{
	builder::CustomOperators,
	catalog::FlowCatalog,
	deferred::{
		actor::{FlowActor, FlowActorParams},
		committer::{CommitterMessage, FlowSlice, SliceCommitReply},
		ddl::{extract_deleted_flow_ids, extract_new_flows},
		frontier::ControlFrontier,
		health::FlowHealthRegistry,
		loader::LoaderMessage,
		routing::{self, ViewRoute},
		tracker::{FlowPositionTracker, ObjectVersionTracker},
	},
	operator::metrics::OperatorSampleRegistry,
};

const FLOW_RETRY_LIMIT: u32 = 3;

const FLOW_RETRY_BACKOFF_MS: u64 = 50;

pub struct FlowSupervisorParams {
	pub engine: StandardEngine,
	pub flow_catalog: FlowCatalog,
	pub committer: ActorRef<CommitterMessage>,
	pub backlog: FlowBacklog,
	pub loader: ActorRef<LoaderMessage>,
	pub control: ControlFrontier,
	pub poll_frontier: CdcConsumerWatermark,
	pub view_lineage: ViewLineage,
	pub tracker: ObjectVersionTracker,
	pub flow_tracker: FlowPositionTracker,
	pub health: FlowHealthRegistry,
	pub custom_operators: CustomOperators,
	pub substrate: FlowSubstrate,
	pub operator_samples: OperatorSampleRegistry,
	pub state_budget: OperatorStateBudgetHandle,
	pub retention_metrics: RetentionMetrics,
	pub clock: Clock,
	pub spawner: ActorSpawner,
	pub consumer_id: CdcConsumerId,
	pub pull_batch_bytes: ByteSize,
	pub load_batch_bytes: ByteSize,
	pub checkpoint_lag: u64,
	pub checkpoint_max_age: Duration,
}

pub struct FlowSupervisor {
	engine: StandardEngine,
	flow_catalog: FlowCatalog,
	committer: ActorRef<CommitterMessage>,
	backlog: FlowBacklog,
	loader: ActorRef<LoaderMessage>,
	control: ControlFrontier,
	poll_frontier: CdcConsumerWatermark,
	view_lineage: ViewLineage,
	tracker: ObjectVersionTracker,
	flow_tracker: FlowPositionTracker,
	health: FlowHealthRegistry,
	custom_operators: CustomOperators,
	substrate: FlowSubstrate,
	operator_samples: OperatorSampleRegistry,
	state_budget: OperatorStateBudgetHandle,
	retention_metrics: RetentionMetrics,
	clock: Clock,
	spawner: ActorSpawner,
	consumer_id: CdcConsumerId,
	pull_batch_bytes: ByteSize,
	load_batch_bytes: ByteSize,
	checkpoint_lag: u64,
	checkpoint_max_age: Duration,
}

pub struct SupervisorState {
	analyzer: FlowGraphAnalyzer,
	flows: BTreeMap<FlowId, FlowActorHandle>,
	sources: BTreeMap<FlowId, Arc<BTreeSet<ObjectId>>>,
	scan_cursor: CommitVersion,
	last_control_commit_at: DateTime,
}

impl FlowSupervisor {
	pub fn new(params: FlowSupervisorParams) -> Self {
		Self {
			engine: params.engine,
			flow_catalog: params.flow_catalog,
			committer: params.committer,
			backlog: params.backlog,
			loader: params.loader,
			control: params.control,
			poll_frontier: params.poll_frontier,
			view_lineage: params.view_lineage,
			tracker: params.tracker,
			flow_tracker: params.flow_tracker,
			health: params.health,
			custom_operators: params.custom_operators,
			substrate: params.substrate,
			operator_samples: params.operator_samples,
			state_budget: params.state_budget,
			retention_metrics: params.retention_metrics,
			clock: params.clock,
			spawner: params.spawner,
			consumer_id: params.consumer_id,
			pull_batch_bytes: params.pull_batch_bytes,
			load_batch_bytes: params.load_batch_bytes,
			checkpoint_lag: params.checkpoint_lag,
			checkpoint_max_age: params.checkpoint_max_age,
		}
	}

	fn handle_bootstrap(&self, state: &mut SupervisorState, flows: Vec<FlowId>, scan_from: Option<CommitVersion>) {
		let migration_base = self.fetch_ddl_cursor().unwrap_or(CommitVersion(0));

		let mut query = match self.engine.begin_query(IdentityId::system()) {
			Ok(q) => q,
			Err(e) => {
				error!(error = %e, "failed to begin query during flow bootstrap");
				return;
			}
		};

		let mut to_spawn: Vec<(FlowDag, CommitVersion)> = Vec::new();
		let mut seeds: Vec<(FlowId, CommitVersion)> = Vec::new();
		for flow_id in flows {
			let flow = match self
				.flow_catalog
				.get_or_load_flow(&mut Transaction::Query(&mut query), flow_id)
			{
				Ok((flow, _)) => flow,
				Err(e) => {
					warn!(flow_id = flow_id.0, error = %e, "failed to load flow during bootstrap, skipping");
					continue;
				}
			};
			self.reject_transactional_flow(&flow);
			state.analyzer.add(flow.clone());
			let seed = CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut query), &flow_id)
				.unwrap_or(None)
				.unwrap_or(migration_base);
			seeds.push((flow_id, seed));
			to_spawn.push((flow, seed));
		}
		drop(query);
		self.publish_lineage(state);

		let scan_cursor = scan_from.unwrap_or(migration_base);
		state.scan_cursor = scan_cursor;
		state.last_control_commit_at = self.clock.now();
		self.control.store(scan_cursor);
		self.poll_frontier.store(scan_cursor);
		self.backlog.set_anchor(scan_cursor);

		self.commit_control(seeds, None);

		let registered: BTreeSet<FlowId> = to_spawn.iter().map(|(f, _)| f.id).collect();
		for (flow, seed) in to_spawn {
			let flow_id = flow.id;
			let source_objects = self.compute_source_objects(state, flow_id, &registered);
			state.sources.insert(flow_id, source_objects.clone());
			let handle = self.spawn_flow(flow, source_objects, seed);
			state.flows.insert(flow_id, handle);
			debug!(flow_id = flow_id.0, seed = seed.0, "spawned deferred flow actor");
		}
	}

	fn handle_wake(&self, state: &mut SupervisorState, ctx: &Context<FlowSupervisorMessage>) {
		self.backlog.disarm();

		let watermark = self.engine.cdc_producer_watermark();
		let done_until = self.engine.done_until();
		let bound = watermark.min(done_until);
		if watermark > done_until {
			let self_ref = ctx.self_ref().clone();
			self.engine.notify_on_mark(
				watermark,
				Box::new(move || {
					let _ = self_ref.send(FlowSupervisorMessage::Wake);
				}),
			);
		}
		if bound <= state.scan_cursor {
			return;
		}

		let items = match self.backlog.pull(state.scan_cursor, bound, ByteSize::from_bytes(u64::MAX)) {
			BacklogPull::Hit {
				items,
				..
			} => items,
			BacklogPull::Behind => {
				error!(
					scan_cursor = state.scan_cursor.0,
					bound = bound.0,
					"flow supervisor fell behind its own backlog anchor; skipping DDL scan"
				);
				return;
			}
		};

		self.update_tracker(&items);
		let seeds = self.process_ddl(state, &items, bound);

		state.scan_cursor = bound;
		self.control.store(bound);
		self.poll_frontier.store(bound);
		self.backlog.set_anchor(bound);

		let eviction_floor = state
			.flows
			.keys()
			.filter_map(|id| self.flow_tracker.all().get(id).copied())
			.min()
			.unwrap_or(bound)
			.min(bound);
		self.backlog.evict_below(eviction_floor);

		let now = self.clock.now();
		if !seeds.is_empty() || now - state.last_control_commit_at >= self.checkpoint_max_age {
			self.commit_control(seeds, Some(bound));
			state.last_control_commit_at = now;
		}

		for handle in state.flows.values() {
			let _ = handle.actor_ref().send(FlowActorMessage::Wake);
		}
	}

	fn process_ddl(
		&self,
		state: &mut SupervisorState,
		items: &[Arc<Cdc>],
		bound: CommitVersion,
	) -> Vec<(FlowId, CommitVersion)> {
		let deleted = extract_deleted_flow_ids(items);
		let mut changed = false;
		let mut lineage_dirty = false;
		for flow_id in &deleted {
			if let Some(handle) = state.flows.remove(flow_id) {
				let _ = handle.actor_ref().send(FlowActorMessage::Stop {
					delete_checkpoint: true,
					reply: Box::new(|| {}),
				});
				changed = true;
			}
			state.sources.remove(flow_id);
			self.health.clear(*flow_id);
			self.flow_catalog.remove(*flow_id);
			state.analyzer.remove(*flow_id);
			lineage_dirty = true;
		}

		let mut seeds: Vec<(FlowId, CommitVersion)> = Vec::new();
		let mut to_spawn: Vec<(FlowDag, CommitVersion)> = Vec::new();
		for (flow_id, version) in extract_new_flows(items) {
			if deleted.contains(&flow_id) {
				self.flow_catalog.remove(flow_id);
				continue;
			}
			if state.flows.contains_key(&flow_id) {
				continue;
			}
			let Some((flow, is_new)) = self.load_flow_at(flow_id, version) else {
				continue;
			};
			self.reject_transactional_flow(&flow);
			if !is_new {
				state.analyzer.add(flow);
				self.flow_catalog.remove(flow_id);
				lineage_dirty = true;
				continue;
			}
			state.analyzer.add(flow.clone());
			lineage_dirty = true;
			let seed = if flow.is_subscription() {
				bound
			} else {
				CommitVersion(0)
			};
			seeds.push((flow_id, seed));
			to_spawn.push((flow, seed));
			changed = true;
		}

		if lineage_dirty {
			self.publish_lineage(state);
		}

		let registered: BTreeSet<FlowId> =
			state.flows.keys().copied().chain(to_spawn.iter().map(|(f, _)| f.id)).collect();
		for (flow, seed) in to_spawn {
			let flow_id = flow.id;
			let source_objects = self.compute_source_objects(state, flow_id, &registered);
			state.sources.insert(flow_id, source_objects.clone());
			let handle = self.spawn_flow(flow, source_objects, seed);
			state.flows.insert(flow_id, handle);
			debug!(flow_id = flow_id.0, seed = seed.0, "spawned new deferred flow actor");
		}

		if changed {
			let registered: BTreeSet<FlowId> = state.flows.keys().copied().collect();
			let flow_ids: Vec<FlowId> = state.flows.keys().copied().collect();
			for flow_id in flow_ids {
				let source_objects = self.compute_source_objects(state, flow_id, &registered);
				state.sources.insert(flow_id, source_objects.clone());
				if let Some(handle) = state.flows.get(&flow_id) {
					let _ = handle.actor_ref().send(FlowActorMessage::UpdateSources {
						source_objects,
					});
				}
			}
		}

		seeds
	}

	fn load_flow_at(&self, flow_id: FlowId, version: CommitVersion) -> Option<(FlowDag, bool)> {
		let lease = match self.engine.acquire_version_lease(version) {
			Ok(lease) => lease,
			Err(e) if e.0.code == "TXN_012" => match self.engine.acquire_current_snapshot_lease() {
				Ok((_, lease)) => lease,
				Err(e) => {
					warn!(flow_id = flow_id.0, error = %e, "failed to lease snapshot for new flow, skipping");
					return None;
				}
			},
			Err(e) => {
				warn!(flow_id = flow_id.0, error = %e, "failed to lease creation version for new flow, skipping");
				return None;
			}
		};
		let mut query = match self.engine.begin_query_at_version(&lease, IdentityId::system()) {
			Ok(q) => q,
			Err(e) => {
				warn!(flow_id = flow_id.0, error = %e, "failed to begin query for new flow, skipping");
				return None;
			}
		};
		match self.flow_catalog.get_or_load_flow(&mut Transaction::Query(&mut query), flow_id) {
			Ok(loaded) => Some(loaded),
			Err(e) => {
				warn!(flow_id = flow_id.0, error = %e, "failed to load flow in supervisor, skipping");
				None
			}
		}
	}

	fn reject_transactional_flow(&self, flow: &FlowDag) {
		for operator_id in flow.get_operator_ids() {
			let Some(operator) = flow.get_operator(&operator_id) else {
				continue;
			};
			let view = match &operator.ty {
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
				} => view,
				_ => continue,
			};
			if self.flow_catalog.find_view(*view).is_some_and(|def| def.kind() == ViewKind::Transactional) {
				unimplemented!("transactional view execution; see plan-operator.md follow-up");
			}
		}
	}

	fn publish_lineage(&self, state: &SupervisorState) {
		self.view_lineage.publish(state.analyzer.get_dependency_graph().upstream_closure());
	}

	fn compute_source_objects(
		&self,
		state: &SupervisorState,
		flow_id: FlowId,
		registered: &BTreeSet<FlowId>,
	) -> Arc<BTreeSet<ObjectId>> {
		let graph = state.analyzer.get_dependency_graph();
		let is_registered = |f: FlowId| registered.contains(&f);
		let view_route = |view_id| {
			self.flow_catalog.find_view(view_id).map(|v| ViewRoute {
				kind: v.kind(),
				storage: v.storage_id(),
			})
		};
		Arc::new(routing::flow_source_objects(graph, flow_id, &is_registered, &view_route))
	}

	fn spawn_flow(
		&self,
		flow: FlowDag,
		source_objects: Arc<BTreeSet<ObjectId>>,
		cursor: CommitVersion,
	) -> FlowActorHandle {
		let flow_id = flow.id;

		self.flow_tracker.update(flow_id, cursor);
		let params = FlowActorParams {
			engine: self.engine.clone(),
			committer: self.committer.clone(),
			backlog: self.backlog.clone(),
			loader: self.loader.clone(),
			control: self.control.clone(),
			custom_operators: self.custom_operators.clone(),
			substrate: self.substrate.clone(),
			operator_samples: self.operator_samples.clone(),
			state_budget: self.state_budget.clone(),
			retention_metrics: self.retention_metrics.clone(),
			clock: self.clock.clone(),
			health: self.health.clone(),
			flow_tracker: self.flow_tracker.clone(),
			flow,
			source_objects,
			cursor,
			pull_batch_bytes: self.pull_batch_bytes,
			load_batch_bytes: self.load_batch_bytes,
			checkpoint_lag: self.checkpoint_lag,
			checkpoint_max_age: self.checkpoint_max_age,
			retry_limit: FLOW_RETRY_LIMIT,
			retry_backoff: Duration::from_milliseconds(FLOW_RETRY_BACKOFF_MS as i64).unwrap(),
		};
		self.spawner.spawn_flow(&format!("flow-{}", flow_id.0), FlowActor::new(params))
	}

	fn commit_control(&self, seeds: Vec<(FlowId, CommitVersion)>, cursor: Option<CommitVersion>) {
		if seeds.is_empty() && cursor.is_none() {
			return;
		}
		let mut slice = FlowSlice::empty();
		slice.checkpoints = seeds;
		slice.control_cursor = cursor.map(|v| (self.consumer_id.clone(), v));
		let reply: SliceCommitReply = Box::new(|_| {});
		let _ = self.committer.send(CommitterMessage::Slice {
			slice,
			reply,
		});
	}

	fn update_tracker(&self, cdcs: &[Arc<Cdc>]) {
		for cdc in cdcs {
			let version = cdc.version;
			for change in &cdc.changes {
				if let ChangeOrigin::Object(source) = &change.origin {
					self.tracker.update(*source, version);
				}
			}
		}
	}

	fn fetch_ddl_cursor(&self) -> Result<CommitVersion> {
		let mut query = self.engine.begin_query(IdentityId::system())?;
		Ok(CdcCheckpoint::fetch(&mut Transaction::Query(&mut query), &self.consumer_id)
			.unwrap_or(CommitVersion(0)))
	}
}

impl Actor for FlowSupervisor {
	type State = SupervisorState;
	type Message = FlowSupervisorMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		SupervisorState {
			analyzer: FlowGraphAnalyzer::new(),
			flows: BTreeMap::new(),
			sources: BTreeMap::new(),
			scan_cursor: CommitVersion(0),
			last_control_commit_at: self.clock.now(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		catch_unwind(AssertUnwindSafe(|| match msg {
			FlowSupervisorMessage::Bootstrap {
				flows,
				scan_from,
			} => self.handle_bootstrap(state, flows, scan_from),
			FlowSupervisorMessage::Wake => self.handle_wake(state, ctx),
		}))
		.unwrap_or_else(|_| {
			error!("panic in flow supervisor, aborting");
			process::abort()
		});
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}
