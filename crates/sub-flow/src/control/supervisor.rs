// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
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
		catalog::{flow::FlowId, id::ViewId, object::ObjectId, view::ViewKind},
		cdc::{Cdc, CdcConsumerId},
		change::ChangeOrigin,
	},
};
use reifydb_engine::{engine::StandardEngine, vm::flow_lineage::ViewLineage};
use reifydb_flow::{operator::metrics::OperatorSampleRegistry, transaction::substrate::FlowSubstrate};
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
use reifydb_store_operator::store::OperatorStore;
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
	commit::committer::{CommitterMessage, FlowSlice, SliceCommitReply},
	control::{
		actor::{FlowActor, FlowActorParams},
		health::FlowHealthRegistry,
	},
	discovery::{
		ddl::{extract_deleted_flow_ids, extract_new_flows},
		loader::LoaderMessage,
		routing::{self, ViewRoute},
	},
	progress::{
		frontier::ControlFrontier,
		output_frontier,
		tracker::{FlowPositionTracker, ObjectVersionTracker},
	},
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
	pub clock: Clock,
	pub spawner: ActorSpawner,
	pub consumer_id: CdcConsumerId,
	pub pull_batch_bytes: ByteSize,
	pub load_batch_bytes: ByteSize,
	pub checkpoint_lag: u64,
	pub checkpoint_max_age: Duration,
	pub frontier_persist: Duration,
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
	clock: Clock,
	spawner: ActorSpawner,
	consumer_id: CdcConsumerId,
	pull_batch_bytes: ByteSize,
	load_batch_bytes: ByteSize,
	checkpoint_lag: u64,
	checkpoint_max_age: Duration,
	frontier_persist: Duration,
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
			clock: params.clock,
			spawner: params.spawner,
			consumer_id: params.consumer_id,
			pull_batch_bytes: params.pull_batch_bytes,
			load_batch_bytes: params.load_batch_bytes,
			checkpoint_lag: params.checkpoint_lag,
			checkpoint_max_age: params.checkpoint_max_age,
			frontier_persist: params.frontier_persist,
		}
	}

	fn handle_bootstrap(&self, state: &mut SupervisorState, flows: Vec<FlowId>, scan_from: Option<CommitVersion>) {
		let migration_base = self.fetch_ddl_cursor().unwrap_or(CommitVersion(0));
		let mut known: BTreeSet<FlowId> = flows.iter().copied().collect();

		let mut query = match self.engine.begin_query(IdentityId::system()) {
			Ok(q) => q,
			Err(e) => {
				error!(error = %e, "failed to begin query during flow bootstrap");
				return;
			}
		};

		let operators = self.engine.operator_state();
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
			let seed = operators.checkpoint_get(flow_id).unwrap_or(migration_base);
			seeds.push((flow_id, seed));
			to_spawn.push((flow, seed));
		}

		match self.engine.catalog().list_flows_all(&mut Transaction::Query(&mut query)) {
			Ok(listed) => {
				known.extend(listed.into_iter().map(|flow| flow.id));
				let reaped = reap_orphan_checkpoints(&operators, &known);
				if reaped > 0 {
					debug!(count = reaped, "reaped orphan flow checkpoints");
				}
			}
			Err(e) => {
				warn!(error = %e, "failed to list flows during bootstrap; orphan flow checkpoints stay and keep pinning cdc retention")
			}
		}

		drop(query);
		self.publish_lineage(state);

		self.hydrate_frontiers();

		let scan_cursor = scan_from.unwrap_or(migration_base);
		state.scan_cursor = scan_cursor;
		state.last_control_commit_at = self.clock.now();
		self.control.store(scan_cursor);
		self.poll_frontier.store(scan_cursor);
		self.backlog.set_anchor(scan_cursor);

		self.commit_control(seeds, None);

		let registered: BTreeSet<FlowId> = to_spawn.iter().map(|(f, _)| f.id).collect();
		let closure = state.analyzer.get_dependency_graph().upstream_closure();
		for (flow, seed) in to_spawn {
			let flow_id = flow.id;
			let source_objects = self.compute_source_objects(state, flow_id, &registered);
			let completeness_objects = self.compute_completeness_objects(state, flow_id, &closure);
			state.sources.insert(flow_id, source_objects.clone());
			let handle = self.spawn_flow(flow, source_objects, completeness_objects, seed);
			state.flows.insert(flow_id, handle);
			debug!(flow_id = flow_id.0, seed = seed.0, "spawned deferred flow actor");
		}
	}

	fn hydrate_frontiers(&self) {
		match output_frontier::hydrate(&self.engine.single().read_store()) {
			Ok(entries) => {
				if !entries.is_empty() {
					debug!(count = entries.len(), "hydrated output frontiers");
				}
				self.substrate.frontiers.hydrate(entries);
			}
			Err(e) => {
				warn!(error = %e, "failed to hydrate output frontiers; every consumer stays at the epoch until its producer republishes")
			}
		}
	}

	fn handle_persist_frontiers(&self, ctx: &Context<FlowSupervisorMessage>) {
		output_frontier::sweep(self.engine.single(), &self.substrate.frontiers);
		ctx.schedule_once(self.frontier_persist, || FlowSupervisorMessage::PersistFrontiers);
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
		let operators = self.engine.operator_state();
		let mut changed = false;
		let mut lineage_dirty = false;
		for flow_id in &deleted {
			if retire_flow(&operators, &mut state.flows, *flow_id) {
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
		let closure = state.analyzer.get_dependency_graph().upstream_closure();
		for (flow, seed) in to_spawn {
			let flow_id = flow.id;
			let source_objects = self.compute_source_objects(state, flow_id, &registered);
			let completeness_objects = self.compute_completeness_objects(state, flow_id, &closure);
			state.sources.insert(flow_id, source_objects.clone());
			let handle = self.spawn_flow(flow, source_objects, completeness_objects, seed);
			state.flows.insert(flow_id, handle);
			debug!(flow_id = flow_id.0, seed = seed.0, "spawned new deferred flow actor");
		}

		if changed {
			let registered: BTreeSet<FlowId> = state.flows.keys().copied().collect();
			let flow_ids: Vec<FlowId> = state.flows.keys().copied().collect();
			for flow_id in flow_ids {
				let source_objects = self.compute_source_objects(state, flow_id, &registered);
				let completeness_objects = self.compute_completeness_objects(state, flow_id, &closure);
				state.sources.insert(flow_id, source_objects.clone());
				if let Some(handle) = state.flows.get(&flow_id) {
					let _ = handle.actor_ref().send(FlowActorMessage::UpdateSources {
						source_objects,
						completeness_objects,
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

	fn compute_completeness_objects(
		&self,
		state: &SupervisorState,
		flow_id: FlowId,
		closure: &BTreeMap<ViewId, BTreeSet<ObjectId>>,
	) -> Option<Arc<BTreeSet<u64>>> {
		let graph = state.analyzer.get_dependency_graph();
		routing::flow_completeness_objects(graph, flow_id, closure).map(Arc::new)
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
		completeness_objects: Option<Arc<BTreeSet<u64>>>,
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
			clock: self.clock.clone(),
			health: self.health.clone(),
			flow_tracker: self.flow_tracker.clone(),
			flow,
			source_objects,
			completeness_objects,
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

fn retire_flow(operators: &OperatorStore, flows: &mut BTreeMap<FlowId, FlowActorHandle>, flow_id: FlowId) -> bool {
	let Some(handle) = flows.remove(&flow_id) else {
		operators.checkpoint_delete(flow_id);
		return false;
	};
	if handle
		.actor_ref()
		.send(FlowActorMessage::Stop {
			delete_checkpoint: true,
			reply: Box::new(|| {}),
		})
		.is_err()
	{
		operators.checkpoint_delete(flow_id);
	}
	true
}

fn reap_orphan_checkpoints(operators: &OperatorStore, known: &BTreeSet<FlowId>) -> usize {
	let mut reaped = 0;
	for flow_id in operators.checkpoint_list() {
		if known.contains(&flow_id) {
			continue;
		}
		operators.checkpoint_delete(flow_id);
		reaped += 1;
	}
	reaped
}

impl Actor for FlowSupervisor {
	type State = SupervisorState;
	type Message = FlowSupervisorMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.frontier_persist, || FlowSupervisorMessage::PersistFrontiers);
		SupervisorState {
			analyzer: FlowGraphAnalyzer::new(),
			flows: BTreeMap::new(),
			sources: BTreeMap::new(),
			scan_cursor: CommitVersion(0),
			last_control_commit_at: self.clock.now(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			FlowSupervisorMessage::Bootstrap {
				flows,
				scan_from,
			} => self.handle_bootstrap(state, flows, scan_from),
			FlowSupervisorMessage::Wake => self.handle_wake(state, ctx),
			FlowSupervisorMessage::PersistFrontiers => self.handle_persist_frontiers(ctx),
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::{BTreeMap, BTreeSet},
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
	};

	use reifydb_core::{actors::flow::FlowActorMessage, common::CommitVersion, interface::catalog::flow::FlowId};
	use reifydb_runtime::{
		actor::{
			context::Context,
			system::{ActorConfig, ActorSystem},
			traits::{Actor, Directive},
		},
		context::clock::Clock,
		sync::waiter::WaiterHandle,
	};
	use reifydb_store_operator::store::OperatorStore;
	use reifydb_value::value::duration::Duration;

	use super::{reap_orphan_checkpoints, retire_flow};

	struct StopRecorder {
		deleted: Arc<AtomicBool>,
		waiter: Arc<WaiterHandle>,
	}

	impl Actor for StopRecorder {
		type State = ();
		type Message = FlowActorMessage;

		fn init(&self, _ctx: &Context<Self::Message>) {}

		fn handle(&self, _state: &mut (), msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
			if let FlowActorMessage::Stop {
				delete_checkpoint,
				..
			} = msg
			{
				self.deleted.store(delete_checkpoint, Ordering::Release);
				self.waiter.notify();
			}
			Directive::Continue
		}

		fn config(&self) -> ActorConfig {
			ActorConfig::new()
		}
	}

	#[test]
	fn bootstrap_reaps_a_checkpoint_whose_flow_the_catalog_no_longer_lists() {
		// the retention floor is the minimum over these rows, so one orphan pins cdc truncation forever
		let store = OperatorStore::testing_memory();
		store.checkpoint_set(FlowId(1), CommitVersion(10));
		store.checkpoint_set(FlowId(2), CommitVersion(20));

		let reaped = reap_orphan_checkpoints(&store, &BTreeSet::from([FlowId(1)]));

		assert_eq!(reaped, 1, "exactly the row with no flow behind it is the one that must go");
		assert_eq!(
			store.checkpoint_get(FlowId(1)),
			Some(CommitVersion(10)),
			"a live flow's checkpoint is its resume point; reaping it in the same pass replays every \
			 slice the flow ever consumed and double-counts every aggregate"
		);
		assert!(
			store.checkpoint_get(FlowId(2)).is_none(),
			"the orphan must be deleted, otherwise its version stays the floor and no cdc entry in the \
			 database is ever reaped again"
		);
	}

	#[test]
	fn retiring_a_live_flow_leaves_the_delete_to_the_stop_message() {
		// an out-of-band delete loses to the completion of a slice already in flight, stranding the row
		let store = OperatorStore::testing_memory();
		store.checkpoint_set(FlowId(1), CommitVersion(10));

		let actor_system = ActorSystem::testing(Clock::testing());
		let deleted = Arc::new(AtomicBool::new(false));
		let waiter = Arc::new(WaiterHandle::new());
		let handle = actor_system.spawner().spawn_flow(
			"retire-test",
			StopRecorder {
				deleted: Arc::clone(&deleted),
				waiter: Arc::clone(&waiter),
			},
		);

		let mut flows = BTreeMap::new();
		flows.insert(FlowId(1), handle);

		let stopped = retire_flow(&store, &mut flows, FlowId(1));

		assert!(stopped, "a live handle must be stopped");
		assert_eq!(
			store.checkpoint_get(FlowId(1)),
			Some(CommitVersion(10)),
			"the supervisor must not delete out of band; only the stop slice may, because it is ordered \
			 behind every slice the flow already has in flight"
		);
		assert!(waiter.wait_timeout(Duration::from_seconds(5).unwrap()), "the stop message must arrive");
		assert!(
			deleted.load(Ordering::Acquire),
			"the stop must carry the delete, or nothing ever removes the row"
		);
	}

	#[test]
	fn a_dropped_flow_loses_its_checkpoint_even_when_this_process_holds_no_actor_for_it() {
		// the stop message carries the delete, but a flow dropped before its actor spawned never gets one
		let store = OperatorStore::testing_memory();
		store.checkpoint_set(FlowId(1), CommitVersion(10));
		let mut flows = BTreeMap::new();

		let stopped = retire_flow(&store, &mut flows, FlowId(1));

		assert!(!stopped, "there is no live handle to stop, which is the case the stop path cannot cover");
		assert!(
			store.checkpoint_get(FlowId(1)).is_none(),
			"the delete must not be conditional on the handle; leaving the row behind turns every such \
			 drop into a permanent pin on cdc retention"
		);
	}
}
