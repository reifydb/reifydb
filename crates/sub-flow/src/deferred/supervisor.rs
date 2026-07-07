// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
};

use reifydb_cdc::{
	consume::{checkpoint::CdcCheckpoint, consumer::CdcConsume},
	storage::CdcStore,
};
use reifydb_core::{
	actors::flow::{FlowActorHandle, FlowActorMessage, FlowSupervisorMessage},
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		cdc::{Cdc, CdcConsumerId},
		change::ChangeOrigin,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, flow::FlowDag};
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
	value::{duration::Duration, identity::IdentityId},
};
use tracing::{debug, error, warn};

use crate::{
	builder::CustomOperators,
	catalog::FlowCatalog,
	deferred::{
		actor::{FlowActor, FlowActorParams},
		committer::{CommitterMessage, FlowSlice},
		ddl::{extract_deleted_flow_ids, extract_new_flows},
		health::FlowHealthRegistry,
		routing::{self, ViewRoute},
		tracker::{FlowPositionTracker, ShapeVersionTracker},
	},
	error::FlowDispatchError,
	transaction::allocators::FlowAllocators,
};

const FLOW_RETRY_LIMIT: u32 = 3;

const FLOW_RETRY_BACKOFF_MS: u64 = 50;

pub(crate) struct FlowConsumeRef {
	pub actor_ref: ActorRef<FlowSupervisorMessage>,
}

impl CdcConsume for FlowConsumeRef {
	fn consume(&self, cdcs: Vec<Cdc>, reply: Box<dyn FnOnce(Result<()>) + Send>) {
		let current_version = cdcs.last().map(|c| c.version).unwrap_or(CommitVersion(0));
		let result = self.actor_ref.send(FlowSupervisorMessage::Consume {
			cdcs,
			current_version,
			reply,
		});
		if let Err(send_err) = result
			&& let FlowSupervisorMessage::Consume {
				reply,
				..
			} = send_err.into_inner()
		{
			reply(Err(FlowDispatchError::SupervisorStopped.into()));
		}
	}
}

pub struct FlowSupervisor {
	engine: StandardEngine,
	flow_catalog: FlowCatalog,
	committer: ActorRef<CommitterMessage>,
	cdc_store: CdcStore,
	tracker: ShapeVersionTracker,
	flow_tracker: FlowPositionTracker,
	health: FlowHealthRegistry,
	custom_operators: CustomOperators,
	allocators: FlowAllocators,
	clock: Clock,
	spawner: ActorSpawner,
	consumer_id: CdcConsumerId,
	chunk_size: u64,
	checkpoint_lag: u64,
}

pub struct SupervisorState {
	analyzer: FlowGraphAnalyzer,
	flows: BTreeMap<FlowId, FlowActorHandle>,
	sources: BTreeMap<FlowId, Arc<BTreeSet<ShapeId>>>,
	frontier: Option<CommitVersion>,
}

impl FlowSupervisor {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		engine: StandardEngine,
		flow_catalog: FlowCatalog,
		committer: ActorRef<CommitterMessage>,
		cdc_store: CdcStore,
		tracker: ShapeVersionTracker,
		flow_tracker: FlowPositionTracker,
		health: FlowHealthRegistry,
		custom_operators: CustomOperators,
		allocators: FlowAllocators,
		clock: Clock,
		spawner: ActorSpawner,
		consumer_id: CdcConsumerId,
		chunk_size: u64,
		checkpoint_lag: u64,
	) -> Self {
		Self {
			engine,
			flow_catalog,
			committer,
			cdc_store,
			tracker,
			flow_tracker,
			health,
			custom_operators,
			allocators,
			clock,
			spawner,
			consumer_id,
			chunk_size,
			checkpoint_lag,
		}
	}

	fn handle_bootstrap(&self, state: &mut SupervisorState, flows: Vec<(FlowId, bool)>) {
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
		for (flow_id, is_deferred) in flows {
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
			state.analyzer.add(flow.clone());
			if !is_deferred {
				continue;
			}
			let seed = CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut query), &flow_id)
				.unwrap_or(None)
				.unwrap_or(migration_base);
			seeds.push((flow_id, seed));
			to_spawn.push((flow, seed));
		}
		drop(query);

		self.commit_control(seeds, None);

		let registered: BTreeSet<FlowId> = to_spawn.iter().map(|(f, _)| f.id).collect();
		for (flow, seed) in to_spawn {
			let flow_id = flow.id;
			let source_shapes = self.compute_source_shapes(state, flow_id, &registered);
			state.sources.insert(flow_id, source_shapes.clone());
			let handle = self.spawn_flow(flow, source_shapes, seed);
			state.flows.insert(flow_id, handle);
			debug!(flow_id = flow_id.0, seed = seed.0, "spawned deferred flow actor");
		}
	}

	fn handle_consume(
		&self,
		state: &mut SupervisorState,
		cdcs: Vec<Cdc>,
		current_version: CommitVersion,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	) {
		self.update_tracker(&cdcs);

		let deleted = extract_deleted_flow_ids(&cdcs);
		let mut changed = false;
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
		}

		let new_flows = match self.discover_and_load_new_flows(state, &cdcs, &deleted) {
			Ok(flows) => flows,
			Err(e) => {
				(reply)(Err(e));
				return;
			}
		};

		let mut seeds: Vec<(FlowId, CommitVersion)> = Vec::new();
		let mut to_spawn: Vec<(FlowDag, CommitVersion)> = Vec::new();
		for flow in new_flows {
			let flow_id = flow.id;
			state.analyzer.add(flow.clone());
			let seed = if flow.is_subscription() {
				current_version
			} else {
				CommitVersion(0)
			};
			seeds.push((flow_id, seed));
			to_spawn.push((flow, seed));
			changed = true;
		}

		self.commit_control(seeds, Some(current_version));

		let registered: BTreeSet<FlowId> =
			state.flows.keys().copied().chain(to_spawn.iter().map(|(f, _)| f.id)).collect();
		for (flow, seed) in to_spawn {
			let flow_id = flow.id;
			let source_shapes = self.compute_source_shapes(state, flow_id, &registered);
			state.sources.insert(flow_id, source_shapes.clone());
			let handle = self.spawn_flow(flow, source_shapes, seed);
			state.flows.insert(flow_id, handle);
			debug!(flow_id = flow_id.0, seed = seed.0, "spawned new deferred flow actor");
		}

		if changed {
			let registered: BTreeSet<FlowId> = state.flows.keys().copied().collect();
			let flow_ids: Vec<FlowId> = state.flows.keys().copied().collect();
			for flow_id in flow_ids {
				let source_shapes = self.compute_source_shapes(state, flow_id, &registered);
				state.sources.insert(flow_id, source_shapes.clone());
				if let Some(handle) = state.flows.get(&flow_id) {
					let _ = handle.actor_ref().send(FlowActorMessage::UpdateSources {
						source_shapes,
					});
				}
			}
		}

		let (changed_shapes, broadcast) = batch_targets(&cdcs);
		let covers_from = state.frontier;
		let cdcs = Arc::new(cdcs);
		for (flow_id, handle) in &state.flows {
			let relevant = broadcast
				|| state.sources
					.get(flow_id)
					.map_or(true, |shapes| shapes.intersection(&changed_shapes).next().is_some());
			if !relevant {
				continue;
			}
			match covers_from {
				Some(covers_from) => {
					let _ = handle.actor_ref().send(FlowActorMessage::Ingest {
						cdcs: cdcs.clone(),
						covers_from,
						up_to: current_version,
					});
				}
				None => {
					let _ = handle.actor_ref().send(FlowActorMessage::Wake);
				}
			}
		}
		state.frontier = Some(current_version);

		(reply)(Ok(()));
	}

	fn discover_and_load_new_flows(
		&self,
		state: &mut SupervisorState,
		cdcs: &[Cdc],
		deleted: &[FlowId],
	) -> Result<Vec<FlowDag>> {
		let new_flows_at_version = extract_new_flows(cdcs);
		let mut new_flows = Vec::new();
		if new_flows_at_version.is_empty() {
			return Ok(new_flows);
		}

		for (flow_id, version) in new_flows_at_version {
			if deleted.contains(&flow_id) {
				self.flow_catalog.remove(flow_id);
				continue;
			}
			let lease = match self.engine.acquire_version_lease(version) {
				Ok(lease) => lease,
				Err(e) if e.0.code == "TXN_012" => match self.engine.acquire_current_snapshot_lease() {
					Ok((_, lease)) => lease,
					Err(e) => {
						warn!(flow_id = flow_id.0, error = %e, "failed to lease snapshot for new flow, skipping");
						continue;
					}
				},
				Err(e) => {
					warn!(flow_id = flow_id.0, error = %e, "failed to lease creation version for new flow, skipping");
					continue;
				}
			};
			let mut query = self.engine.begin_query_at_version(&lease, IdentityId::system())?;
			match self.flow_catalog.get_or_load_flow(&mut Transaction::Query(&mut query), flow_id) {
				Ok((flow, is_new)) => {
					if is_new {
						new_flows.push(flow);
					} else {
						state.analyzer.add(flow);
						self.flow_catalog.remove(flow_id);
					}
				}
				Err(e) => {
					warn!(flow_id = flow_id.0, error = %e, "failed to load flow in supervisor, skipping");
					continue;
				}
			}
		}
		Ok(new_flows)
	}

	fn compute_source_shapes(
		&self,
		state: &SupervisorState,
		flow_id: FlowId,
		registered: &BTreeSet<FlowId>,
	) -> Arc<BTreeSet<ShapeId>> {
		let graph = state.analyzer.get_dependency_graph();
		let is_registered = |f: FlowId| registered.contains(&f);
		let view_route = |view_id| {
			self.flow_catalog.find_view(view_id).map(|v| ViewRoute {
				kind: v.kind(),
				underlying: v.underlying_id(),
			})
		};
		Arc::new(routing::flow_source_shapes(graph, flow_id, &is_registered, &view_route))
	}

	fn spawn_flow(
		&self,
		flow: FlowDag,
		source_shapes: Arc<BTreeSet<ShapeId>>,
		cursor: CommitVersion,
	) -> FlowActorHandle {
		let flow_id = flow.id;

		self.flow_tracker.update(flow_id, cursor);
		let params = FlowActorParams {
			engine: self.engine.clone(),
			committer: self.committer.clone(),
			cdc_store: self.cdc_store.clone(),
			custom_operators: self.custom_operators.clone(),
			allocators: self.allocators.clone(),
			clock: self.clock.clone(),
			health: self.health.clone(),
			flow_tracker: self.flow_tracker.clone(),
			flow,
			source_shapes,
			cursor,
			chunk_size: self.chunk_size,
			checkpoint_lag: self.checkpoint_lag,
			retry_limit: FLOW_RETRY_LIMIT,
			retry_backoff: Duration::from_milliseconds(FLOW_RETRY_BACKOFF_MS as i64).unwrap(),
		};
		self.spawner.spawn_system(&format!("flow-{}", flow_id.0), FlowActor::new(params))
	}

	fn commit_control(&self, seeds: Vec<(FlowId, CommitVersion)>, cursor: Option<CommitVersion>) {
		if seeds.is_empty() && cursor.is_none() {
			return;
		}
		let mut slice = FlowSlice::empty();
		slice.checkpoints = seeds;
		slice.control_cursor = cursor.map(|v| (self.consumer_id.clone(), v));
		let reply: Box<dyn FnOnce(Result<()>) + Send> = Box::new(|_| {});
		let _ = self.committer.send(CommitterMessage::Slice {
			slice,
			reply,
		});
	}

	fn update_tracker(&self, cdcs: &[Cdc]) {
		for cdc in cdcs {
			let version = cdc.version;
			for change in &cdc.changes {
				if let ChangeOrigin::Shape(source) = &change.origin {
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
			frontier: None,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		catch_unwind(AssertUnwindSafe(|| match msg {
			FlowSupervisorMessage::Bootstrap {
				flows,
			} => self.handle_bootstrap(state, flows),
			FlowSupervisorMessage::Consume {
				cdcs,
				current_version,
				reply,
			} => self.handle_consume(state, cdcs, current_version, reply),
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

fn batch_targets(cdcs: &[Cdc]) -> (BTreeSet<ShapeId>, bool) {
	let mut shapes = BTreeSet::new();
	let mut broadcast = false;
	for cdc in cdcs {
		for change in &cdc.changes {
			match &change.origin {
				ChangeOrigin::Shape(shape) => {
					shapes.insert(*shape);
				}
				ChangeOrigin::Flow(_) => {
					broadcast = true;
				}
			}
		}
	}
	(shapes, broadcast)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{
			catalog::{
				flow::FlowNodeId,
				id::{TableId, ViewId},
			},
			change::{Change, Diff},
		},
		value::column::columns::Columns,
	};
	use reifydb_value::value::datetime::DateTime;
	use smallvec::smallvec;

	use super::*;

	fn change(origin: ChangeOrigin) -> Change {
		Change {
			origin,
			version: CommitVersion(1),
			diffs: smallvec![Diff::Insert {
				post: Columns::empty(),
				origin: None,
			}],
			changed_at: DateTime::default(),
		}
	}

	fn cdc(version: u64, changes: Vec<Change>) -> Cdc {
		Cdc {
			version: CommitVersion(version),
			timestamp: DateTime::default(),
			changes,
			system_changes: Vec::new(),
		}
	}

	#[test]
	fn collects_shape_origins_and_ignores_broadcast() {
		let cdcs = vec![
			cdc(5, vec![change(ChangeOrigin::Shape(ShapeId::Table(TableId(1))))]),
			cdc(6, vec![change(ChangeOrigin::Shape(ShapeId::View(ViewId(2))))]),
		];

		let (shapes, broadcast) = batch_targets(&cdcs);

		assert!(!broadcast);
		assert_eq!(
			shapes.into_iter().collect::<Vec<_>>(),
			vec![ShapeId::Table(TableId(1)), ShapeId::View(ViewId(2))]
		);
	}

	#[test]
	fn flow_origin_forces_broadcast() {
		let cdcs = vec![cdc(
			5,
			vec![
				change(ChangeOrigin::Shape(ShapeId::Table(TableId(1)))),
				change(ChangeOrigin::Flow(FlowNodeId(42))),
			],
		)];

		let (shapes, broadcast) = batch_targets(&cdcs);

		assert!(broadcast, "a flow-origin change must fall back to broadcasting all flows");
		assert!(shapes.contains(&ShapeId::Table(TableId(1))));
	}

	#[test]
	fn empty_batch_targets_nothing() {
		let (shapes, broadcast) = batch_targets(&[]);
		assert!(shapes.is_empty());
		assert!(!broadcast);
	}
}
