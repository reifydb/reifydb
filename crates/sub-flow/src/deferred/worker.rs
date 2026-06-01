// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	sync::Arc,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::{
		flow::{FlowMessage, FlowResponse, WorkerBatch},
		pending::Pending,
	},
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		change::{Change, ChangeOrigin},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	sync::mutex::Mutex,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, identity::IdentityId},
};
use smallvec::smallvec;
use tracing::{Span, error, field, instrument};

use crate::{
	catalog::FlowCatalog,
	engine::FlowEngine,
	transaction::{DeferredParams, FlowTransaction},
};

pub type FlowEngineFactory = Box<dyn FnOnce() -> FlowEngine + Send>;

pub struct FlowWorkerActor {
	engine: StandardEngine,
	catalog: Catalog,
	flow_catalog: FlowCatalog,
	engine_factory: Mutex<Option<FlowEngineFactory>>,
}

impl FlowWorkerActor {
	pub fn new<F>(engine_factory: F, engine: StandardEngine, catalog: Catalog, flow_catalog: FlowCatalog) -> Self
	where
		F: FnOnce() -> FlowEngine + Send + 'static,
	{
		Self {
			engine,
			catalog,
			flow_catalog,
			engine_factory: Mutex::new(Some(Box::new(engine_factory))),
		}
	}
}

pub struct FlowState {
	flow_engine: FlowEngine,
}

impl Actor for FlowWorkerActor {
	type State = FlowState;
	type Message = FlowMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		let factory = self.engine_factory.lock().take();
		let flow_engine = factory.expect("FlowActor::init called twice")();
		FlowState {
			flow_engine,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		catch_unwind(AssertUnwindSafe(|| {
			match msg {
				FlowMessage::Process {
					batch,
					reply,
				} => self.handle_process(state, batch, reply),
				FlowMessage::Dispatch {
					state_version,
					to_version,
					changes,
					index,
					active,
					reply,
				} => self.handle_dispatch(
					state,
					state_version,
					to_version,
					changes,
					index,
					active,
					reply,
				),
				FlowMessage::Tick {
					flow_ids,
					timestamp,
					state_version,
					reply,
				} => self.handle_tick(state, flow_ids, timestamp, state_version, reply),
				FlowMessage::Register {
					flow_id,
					reply,
				} => self.handle_register(state, flow_id, reply),
				FlowMessage::Rebalance {
					flow_ids,
					reply,
				} => self.handle_rebalance(state, flow_ids, reply),
			}
			Directive::Continue
		}))
		.unwrap_or_else(|_| {
			error!("panic in flow worker actor, aborting");
			process::abort()
		})
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl FlowWorkerActor {
	#[inline]
	fn handle_process(
		&self,
		state: &mut FlowState,
		batch: WorkerBatch,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	) {
		let resp = match self.process_request(&mut state.flow_engine, batch) {
			Ok((pending, pending_shapes, view_changes)) => FlowResponse::Success {
				pending,
				pending_shapes,
				view_changes,
			},
			Err(e) => FlowResponse::Error(e.to_string()),
		};
		(reply)(resp);
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn handle_dispatch(
		&self,
		state: &mut FlowState,
		state_version: CommitVersion,
		to_version: CommitVersion,
		changes: Arc<Vec<Change>>,
		index: Arc<HashMap<ShapeId, Vec<FlowId>>>,
		active: Arc<Vec<FlowId>>,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	) {
		let resp = match self.process_dispatch(
			&mut state.flow_engine,
			state_version,
			to_version,
			&changes,
			&index,
			&active,
		) {
			Ok((pending, pending_shapes, view_changes)) => FlowResponse::Success {
				pending,
				pending_shapes,
				view_changes,
			},
			Err(e) => FlowResponse::Error(e.to_string()),
		};
		(reply)(resp);
	}

	#[inline]
	fn handle_tick(
		&self,
		state: &mut FlowState,
		flow_ids: Vec<FlowId>,
		timestamp: DateTime,
		state_version: CommitVersion,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	) {
		let resp = match self.process_tick(&mut state.flow_engine, flow_ids, timestamp, state_version) {
			Ok((pending, pending_shapes)) => FlowResponse::Success {
				pending,
				pending_shapes,
				view_changes: Vec::new(),
			},
			Err(e) => FlowResponse::Error(e.to_string()),
		};
		(reply)(resp);
	}

	#[inline]
	fn handle_register(&self, state: &mut FlowState, flow_id: FlowId, reply: Box<dyn FnOnce(FlowResponse) + Send>) {
		if state.flow_engine.flows.contains_key(&flow_id) {
			(reply)(FlowResponse::Success {
				pending: Pending::new(),
				pending_shapes: Vec::new(),
				view_changes: Vec::new(),
			});
			return;
		}

		let result = self.engine.begin_command(IdentityId::system()).and_then(|mut txn| {
			let (flow, _) =
				self.flow_catalog.get_or_load_flow(&mut Transaction::Command(&mut txn), flow_id)?;
			state.flow_engine.register(&mut txn, flow.into())
		});

		let resp = match result {
			Ok(_) => FlowResponse::Success {
				pending: Pending::new(),
				pending_shapes: Vec::new(),
				view_changes: Vec::new(),
			},
			Err(e) => FlowResponse::Error(e.to_string()),
		};
		(reply)(resp);
	}

	#[inline]
	fn handle_rebalance(
		&self,
		state: &mut FlowState,
		flow_ids: Vec<FlowId>,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	) {
		state.flow_engine.clear();
		let result = self.engine.begin_command(IdentityId::system()).and_then(|mut txn| {
			for fid in flow_ids {
				let (flow, _) =
					self.flow_catalog.get_or_load_flow(&mut Transaction::Command(&mut txn), fid)?;
				state.flow_engine.register(&mut txn, flow.into())?;
			}
			Ok(())
		});

		let resp = match result {
			Ok(_) => FlowResponse::Success {
				pending: Pending::new(),
				pending_shapes: Vec::new(),
				view_changes: Vec::new(),
			},
			Err(e) => FlowResponse::Error(e.to_string()),
		};
		(reply)(resp);
	}

	#[instrument(name = "flow::actor::tick", level = "debug", skip(self, flow_engine, flow_ids), fields(
		flow_count = flow_ids.len(),
		timestamp = %timestamp
	))]
	fn process_tick(
		&self,
		flow_engine: &mut FlowEngine,
		flow_ids: Vec<FlowId>,
		timestamp: DateTime,
		state_version: CommitVersion,
	) -> Result<(Pending, Vec<RowShape>)> {
		let lease = self.engine.multi().acquire_version_lease(state_version)?;
		let query = self.engine.multi().begin_query_at_version(&lease)?;
		let state_query = self.engine.multi().begin_query_at_version(&lease)?;
		let interceptors = self.engine.create_interceptors();

		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: state_version,
			pending: Pending::new(),
			query,
			state_query,
			single: self.engine.single().clone(),
			catalog: self.catalog.clone(),
			interceptors,
			clock: self.engine.clock().clone(),
		});

		for flow_id in flow_ids {
			if let Err(e) = flow_engine.process_tick(&mut txn, flow_id, timestamp) {
				error!(flow_id = flow_id.0, error = %e, "failed to process tick");
			}
		}

		txn.flush_operator_states()?;

		Ok((txn.take_pending(), txn.take_pending_shapes()))
	}

	#[instrument(name = "flow::actor::process", level = "debug", skip(self, flow_engine, batch), fields(
		instructions = batch.instructions.len(),
		total_changes = field::Empty
	))]
	fn process_request(
		&self,
		flow_engine: &mut FlowEngine,
		batch: WorkerBatch,
	) -> Result<(Pending, Vec<RowShape>, Vec<Change>)> {
		let total_changes: usize = batch.instructions.iter().map(|i| i.changes.len()).sum();
		Span::current().record("total_changes", total_changes);

		let mut pending = Pending::new();
		let mut all_pending_shapes: Vec<RowShape> = Vec::new();
		let mut all_view_changes: Vec<Change> = Vec::new();
		let interceptors = self.engine.create_interceptors();

		let state_lease = self.engine.multi().acquire_version_lease(batch.state_version)?;
		let base_query = self.engine.multi().begin_query_at_version(&state_lease)?;
		let base_state_query = self.engine.multi().begin_query_at_version(&state_lease)?;

		for instruction in batch.instructions {
			let flow_id = instruction.flow_id;

			if instruction.changes.is_empty() {
				continue;
			}

			let primitive_version = instruction.to_version;

			let mut query = base_query.clone();
			query.read_as_of_version_inclusive(primitive_version);
			let state_query = base_state_query.clone();

			let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
				version: primitive_version,
				pending,
				query,
				state_query,
				single: self.engine.single().clone(),
				catalog: self.catalog.clone(),
				interceptors: interceptors.clone(),
				clock: self.engine.clock().clone(),
			});

			if let Err(e) = flow_engine.process_batch(&mut txn, instruction.changes.clone(), flow_id) {
				error!(flow_id = flow_id.0, error = %e, "failed to process flow");
			}

			txn.flush_operator_states()?;

			let view_entries = txn.take_accumulator_entries();
			let changed_at = DateTime::from_nanos(self.engine.clock().now_nanos());
			for (id, diff) in view_entries {
				all_view_changes.push(Change {
					origin: ChangeOrigin::Shape(id),
					version: primitive_version,
					diffs: smallvec![diff],
					changed_at,
				});
			}

			all_pending_shapes.extend(txn.take_pending_shapes());
			pending = txn.take_pending();
		}

		Ok((pending, all_pending_shapes, all_view_changes))
	}

	#[instrument(name = "flow::actor::dispatch", level = "debug", skip(self, flow_engine, changes, index, active), fields(
		changes = changes.len(),
		flows = field::Empty
	))]
	fn process_dispatch(
		&self,
		flow_engine: &mut FlowEngine,
		state_version: CommitVersion,
		to_version: CommitVersion,
		changes: &[Change],
		index: &HashMap<ShapeId, Vec<FlowId>>,
		active: &[FlowId],
	) -> Result<(Pending, Vec<RowShape>, Vec<Change>)> {
		let mut per_flow: BTreeMap<FlowId, Vec<Change>> = BTreeMap::new();
		for change in changes {
			match change.origin {
				ChangeOrigin::Shape(source) => {
					if let Some(flows) = index.get(&source) {
						for f in flows {
							if flow_engine.flows.contains_key(f) {
								per_flow.entry(*f).or_default().push(change.clone());
							}
						}
					}
				}
				_ => {
					for f in active {
						if flow_engine.flows.contains_key(f) {
							per_flow.entry(*f).or_default().push(change.clone());
						}
					}
				}
			}
		}
		Span::current().record("flows", per_flow.len());

		let mut pending = Pending::new();
		let mut all_pending_shapes: Vec<RowShape> = Vec::new();
		let mut all_view_changes: Vec<Change> = Vec::new();
		let interceptors = self.engine.create_interceptors();

		let state_lease = self.engine.multi().acquire_version_lease(state_version)?;
		let base_query = self.engine.multi().begin_query_at_version(&state_lease)?;
		let base_state_query = self.engine.multi().begin_query_at_version(&state_lease)?;

		for (flow_id, flow_changes) in per_flow {
			if flow_changes.is_empty() {
				continue;
			}

			let mut query = base_query.clone();
			query.read_as_of_version_inclusive(to_version);
			let state_query = base_state_query.clone();

			let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
				version: to_version,
				pending,
				query,
				state_query,
				single: self.engine.single().clone(),
				catalog: self.catalog.clone(),
				interceptors: interceptors.clone(),
				clock: self.engine.clock().clone(),
			});

			if let Err(e) = flow_engine.process_batch(&mut txn, flow_changes, flow_id) {
				error!(flow_id = flow_id.0, error = %e, "failed to process flow");
			}

			txn.flush_operator_states()?;

			let view_entries = txn.take_accumulator_entries();
			let changed_at = DateTime::from_nanos(self.engine.clock().now_nanos());
			for (id, diff) in view_entries {
				all_view_changes.push(Change {
					origin: ChangeOrigin::Shape(id),
					version: to_version,
					diffs: smallvec![diff],
					changed_at,
				});
			}

			all_pending_shapes.extend(txn.take_pending_shapes());
			pending = txn.take_pending();
		}

		Ok((pending, all_pending_shapes, all_view_changes))
	}
}
