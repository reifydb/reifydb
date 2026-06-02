// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections,
	collections::BTreeMap,
	fmt, mem,
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
	time::Duration,
};

use reifydb_cdc::{
	consume::{checkpoint::CdcCheckpoint, consumer::CdcConsume},
	storage::CdcStore,
};
use reifydb_core::{
	actors::{
		flow::{FlowPoolMessage, PoolResponse},
		pending::Pending,
	},
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowId,
			shape::ShapeId,
		},
		cdc::{Cdc, CdcConsumerId},
		change::Change,
	},
	internal,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, flow::FlowDag};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	context::clock::{Clock, Instant},
};
use reifydb_transaction::{multi::lease::VersionLeaseGuard, transaction::Transaction};
use reifydb_value::{Result, error::Error, value::identity::IdentityId};
use tracing::{error, info, warn};

use super::{
	state::FlowStates,
	tracker::{FlowPositionTracker, ShapeVersionTracker},
};
use crate::catalog::FlowCatalog;

mod backfill_advance;
mod backfill_filter;
mod lifecycle;
mod persist;
mod rebalance;
pub mod registration;
mod tick;

pub(crate) struct FlowConsumeRef {
	pub actor_ref: ActorRef<FlowCoordinatorMessage>,
}

impl CdcConsume for FlowConsumeRef {
	fn consume(&self, cdcs: Vec<Cdc>, reply: Box<dyn FnOnce(Result<()>) + Send>) {
		let current_version = cdcs.last().map(|c| c.version).unwrap_or(CommitVersion(0));

		let result = self.actor_ref.send(FlowCoordinatorMessage::Consume {
			cdcs,
			current_version,
			reply,
		});

		if let Err(send_err) = result
			&& let FlowCoordinatorMessage::Consume {
				reply,
				..
			} = send_err.into_inner()
		{
			reply(Err(Error(Box::new(internal!("Coordinator actor stopped")))));
		}
	}
}

use reifydb_core::actors::flow::FlowCoordinatorMessage;

struct ConsumeContext {
	state_version: CommitVersion,
	current_version: CommitVersion,
	combined: Pending,
	pending_shapes: Vec<RowShape>,
	checkpoints: Vec<(FlowId, CommitVersion)>,
	positions: Vec<(FlowId, CommitVersion)>,
	checkpoint_deletes: Vec<FlowId>,
	original_reply: Box<dyn FnOnce(Result<()>) + Send>,
	consume_start: Instant,

	all_changes: Vec<Change>,

	latest_version: Option<CommitVersion>,

	downstream_flows: collections::HashSet<FlowId>,

	view_changes: Vec<Change>,

	#[allow(dead_code)]
	state_lease: VersionLeaseGuard,
}

impl ConsumeContext {
	fn is_empty(&self) -> bool {
		self.combined.iter_sorted().next().is_none()
			&& self.view_changes.is_empty()
			&& self.checkpoints.is_empty()
			&& self.positions.is_empty()
			&& self.checkpoint_deletes.is_empty()
			&& self.pending_shapes.is_empty()
	}
}

enum Phase {
	Idle,

	RegisteringFlows {
		flows: Vec<FlowDag>,
		ctx: ConsumeContext,
	},

	SubmittingBatches {
		ctx: ConsumeContext,
	},

	AdvancingBackfill {
		flows: Vec<FlowId>,
		ctx: ConsumeContext,
	},

	Rebalancing {
		ctx: ConsumeContext,
	},

	BootstrapRebalancing,

	Ticking {
		#[allow(dead_code)]
		state_lease: VersionLeaseGuard,
	},
}

struct TickSchedule {
	tick: Duration,
	last_tick: Instant,
}

struct PendingConsume {
	cdcs: Vec<Cdc>,
	current_version: CommitVersion,
	reply: Box<dyn FnOnce(Result<()>) + Send>,
}

fn coordinator_error(msg: impl fmt::Display) -> Result<()> {
	Err(Error(Box::new(internal!("{}", msg))))
}

pub struct CoordinatorActor {
	engine: StandardEngine,
	catalog: FlowCatalog,
	pool: ActorRef<FlowPoolMessage>,
	tracker: ShapeVersionTracker,
	flow_tracker: FlowPositionTracker,
	cdc_store: CdcStore,
	num_workers: usize,
	clock: Clock,
	consumer_id: CdcConsumerId,
}

impl CoordinatorActor {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		engine: StandardEngine,
		catalog: FlowCatalog,
		pool_ref: ActorRef<FlowPoolMessage>,
		tracker: ShapeVersionTracker,
		flow_tracker: FlowPositionTracker,
		cdc_store: CdcStore,
		num_workers: usize,
		clock: Clock,
		consumer_id: CdcConsumerId,
	) -> Self {
		Self {
			engine,
			catalog,
			pool: pool_ref,
			tracker,
			flow_tracker,
			cdc_store,
			num_workers,
			clock,
			consumer_id,
		}
	}

	fn flow_tick(&self) -> Duration {
		self.engine.catalog().get_config_duration(ConfigKey::FlowTick)
	}
}

pub struct CoordinatorState {
	states: FlowStates,
	analyzer: FlowGraphAnalyzer,
	phase: Phase,
	phase_entered_at: Option<Instant>,
	pending_consume: Option<PendingConsume>,
	tick_schedules: BTreeMap<FlowId, TickSchedule>,

	flow_assignments: BTreeMap<FlowId, usize>,

	flows_changed: bool,

	cached_active: Arc<Vec<FlowId>>,
	cached_routing_index: Arc<collections::HashMap<ShapeId, Vec<FlowId>>>,
}

impl Actor for CoordinatorActor {
	type State = CoordinatorState;
	type Message = FlowCoordinatorMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.flow_tick(), || FlowCoordinatorMessage::Tick);

		CoordinatorState {
			states: FlowStates::new(),
			analyzer: FlowGraphAnalyzer::new(),
			phase: Phase::Idle,
			phase_entered_at: None,
			pending_consume: None,
			tick_schedules: BTreeMap::new(),
			flow_assignments: BTreeMap::new(),
			flows_changed: false,
			cached_active: Arc::new(Vec::new()),
			cached_routing_index: Arc::new(collections::HashMap::new()),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		catch_unwind(AssertUnwindSafe(|| {
			match msg {
				FlowCoordinatorMessage::Consume {
					cdcs,
					current_version,
					reply,
				} => {
					self.handle_consume(state, ctx, cdcs, current_version, reply);
				}
				FlowCoordinatorMessage::PoolReply(response) => {
					self.handle_pool_reply(state, ctx, response);
				}
				FlowCoordinatorMessage::Bootstrap {
					flows,
				} => {
					self.handle_bootstrap(state, ctx, flows);
				}
				FlowCoordinatorMessage::Tick => {
					if matches!(state.phase, Phase::Idle) {
						self.handle_tick(state, ctx);
					} else if let Some(entered) = &state.phase_entered_at
						&& self.clock.instant().duration_since(entered) > self.flow_tick() * 10
					{
						error!(
							"flow coordinator stuck in non-Idle phase for more than 10x FLOW_TICK; pool reply lost, aborting"
						);
						process::abort();
					}
					ctx.schedule_once(self.flow_tick(), || FlowCoordinatorMessage::Tick);
				}
			}
			Directive::Continue
		}))
		.unwrap_or_else(|_| {
			error!("panic in flow coordinator, aborting");
			process::abort()
		})
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl CoordinatorActor {
	fn handle_pool_reply(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		response: PoolResponse,
	) {
		let phase = mem::replace(&mut state.phase, Phase::Idle);
		state.phase_entered_at = None;
		match phase {
			Phase::RegisteringFlows {
				flows,
				ctx: cctx,
			} => self.continue_registering(state, ctx, response, flows, cctx),
			Phase::Rebalancing {
				ctx: cctx,
			} => self.continue_rebalancing(state, ctx, response, cctx),
			Phase::BootstrapRebalancing => {
				if let PoolResponse::Error(e) = response {
					warn!(error = %e, "bootstrap rebalance failed");
				}
			}
			Phase::SubmittingBatches {
				ctx: cctx,
			} => self.continue_submitting(state, ctx, response, cctx),
			Phase::AdvancingBackfill {
				flows,
				ctx: cctx,
			} => self.continue_advancing_backfill(state, ctx, response, flows, cctx),
			Phase::Ticking {
				..
			} => self.continue_ticking(response),
			Phase::Idle => {}
		}
		self.drain_pending_consume(state, ctx);
	}

	fn handle_bootstrap(
		&self,
		state: &mut CoordinatorState,
		ctx: &Context<FlowCoordinatorMessage>,
		flows: Vec<(FlowId, bool)>,
	) {
		let coordinator_checkpoint = self.fetch_coordinator_checkpoint().unwrap_or(CommitVersion(0));

		let mut query = match self.engine.begin_query(IdentityId::system()) {
			Ok(query) => query,
			Err(e) => {
				error!(error = %e, "failed to begin query during flow bootstrap");
				return;
			}
		};

		for (flow_id, is_deferred) in flows {
			let flow = match self.catalog.get_or_load_flow(&mut Transaction::Query(&mut query), flow_id) {
				Ok((flow, _)) => flow,
				Err(e) => {
					warn!(flow_id = flow_id.0, error = %e, "failed to load flow during bootstrap, skipping");
					continue;
				}
			};

			state.analyzer.add(flow.clone());
			state.flows_changed = true;

			if !is_deferred {
				continue;
			}

			self.maybe_register_tick_schedule(state, &flow);

			let has_durable_checkpoint =
				CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut query), &flow_id)
					.unwrap_or(None)
					.is_some();

			if !flow.is_subscription() && has_durable_checkpoint {
				state.states.register_backfilling(flow_id);
			} else {
				state.states.register_active(flow_id, coordinator_checkpoint);
			}

			info!(
				flow_id = flow_id.0,
				backfilling = has_durable_checkpoint,
				coordinator_checkpoint = coordinator_checkpoint.0,
				"bootstrapped deferred flow on startup"
			);
		}

		let assignments = self.compute_flow_assignments(state);
		if assignments.is_empty() {
			return;
		}
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
			warn!("pool actor stopped during bootstrap rebalance");
			return;
		}

		state.set_phase(Phase::BootstrapRebalancing, self.clock.instant());
	}
}
