// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Flow actor that handles flow processing logic.
//!
//! This module provides an actor-based implementation of flow processing:
//! - [`FlowWorkerActor`]: The actor definition with init/handle methods
//! - [`FlowMsg`]: Messages the actor can receive (Process, Register)
//! - [`FlowResponse`]: Response sent back through callbacks

use std::sync::Mutex;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{
		catalog::flow::FlowId,
		change::{Change, ChangeOrigin},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	traits::{Actor, Directive},
};
use reifydb_type::{
	Result,
	value::{datetime::DateTime, identity::IdentityId},
};
use tracing::{Span, error, field, instrument};

use super::instruction::WorkerBatch;
use crate::{
	engine::FlowEngine,
	transaction::{FlowTransaction, pending::Pending},
};

/// Messages for the flow actor
pub enum FlowMsg {
	/// Process a batch of flow instructions
	Process {
		batch: WorkerBatch,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},
	/// Register a new flow
	Register {
		flow: FlowDag,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},
	/// Process periodic tick for time-based maintenance
	Tick {
		flow_ids: Vec<FlowId>,
		timestamp: DateTime,
		state_version: CommitVersion,
		reply: Box<dyn FnOnce(FlowResponse) + Send>,
	},
}

/// Response from the flow actor
pub enum FlowResponse {
	/// Operation succeeded with pending writes, pending shapes, and view changes
	Success {
		pending: Pending,
		pending_shapes: Vec<RowShape>,
		view_changes: Vec<Change>,
	},
	/// Operation failed with error message
	Error(String),
}

pub type FlowEngineFactory = Box<dyn FnOnce() -> FlowEngine + Send>;

pub struct FlowWorkerActor {
	engine: StandardEngine,
	catalog: Catalog,
	engine_factory: Mutex<Option<FlowEngineFactory>>,
}

impl FlowWorkerActor {
	pub fn new<F>(engine_factory: F, engine: StandardEngine, catalog: Catalog) -> Self
	where
		F: FnOnce() -> FlowEngine + Send + 'static,
	{
		Self {
			engine,
			catalog,
			engine_factory: Mutex::new(Some(Box::new(engine_factory))),
		}
	}
}

pub struct FlowState {
	flow_engine: FlowEngine,
}

impl Actor for FlowWorkerActor {
	type State = FlowState;
	type Message = FlowMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		let factory = self.engine_factory.lock().unwrap().take();
		let flow_engine = factory.expect("FlowActor::init called twice")();
		FlowState {
			flow_engine,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			FlowMsg::Process {
				batch,
				reply,
			} => {
				let result = self.process_request(&mut state.flow_engine, batch);
				let resp = match result {
					Ok((pending, pending_shapes, view_changes)) => FlowResponse::Success {
						pending,
						pending_shapes,
						view_changes,
					},
					Err(e) => FlowResponse::Error(e.to_string()),
				};
				(reply)(resp);
			}
			FlowMsg::Tick {
				flow_ids,
				timestamp,
				state_version,
				reply,
			} => {
				let result =
					self.process_tick(&mut state.flow_engine, flow_ids, timestamp, state_version);
				let resp = match result {
					Ok((pending, pending_shapes)) => FlowResponse::Success {
						pending,
						pending_shapes,
						view_changes: Vec::new(),
					},
					Err(e) => FlowResponse::Error(e.to_string()),
				};
				(reply)(resp);
			}
			FlowMsg::Register {
				flow,
				reply,
			} => {
				let result = self
					.engine
					.begin_command(IdentityId::system())
					.and_then(|mut txn| state.flow_engine.register(&mut txn, flow));

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
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl FlowWorkerActor {
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
		let query = self.engine.multi().begin_query_at_version(state_version)?;
		let state_query = self.engine.multi().begin_query_at_version(state_version)?;
		let interceptors = self.engine.create_interceptors();

		let mut txn = FlowTransaction::deferred_from_parts(
			state_version,
			Pending::new(),
			query,
			state_query,
			self.catalog.clone(),
			interceptors,
			self.engine.clock().clone(),
		);

		for flow_id in flow_ids {
			if let Err(e) = flow_engine.process_tick(&mut txn, flow_id, timestamp) {
				error!(flow_id = flow_id.0, error = %e, "failed to process tick");
			}
		}

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

		for instruction in batch.instructions {
			let flow_id = instruction.flow_id;

			if instruction.changes.is_empty() {
				continue;
			}

			let primitive_version = instruction.to_version;

			let query = self.engine.multi().begin_query_at_version(primitive_version)?;
			let state_query = self.engine.multi().begin_query_at_version(batch.state_version)?;

			let mut txn = FlowTransaction::deferred_from_parts(
				primitive_version,
				pending,
				query,
				state_query,
				self.catalog.clone(),
				interceptors.clone(),
				self.engine.clock().clone(),
			);

			for change in &instruction.changes {
				if let Err(e) = flow_engine.process(&mut txn, change.clone(), flow_id) {
					error!(flow_id = flow_id.0, error = %e, "failed to process flow");
				}
			}

			let view_entries = txn.take_accumulator_entries();
			let changed_at = DateTime::from_nanos(self.engine.clock().now_nanos());
			for (id, diff) in view_entries {
				all_view_changes.push(Change {
					origin: ChangeOrigin::Shape(id),
					version: primitive_version,
					diffs: vec![diff],
					changed_at,
				});
			}

			all_pending_shapes.extend(txn.take_pending_shapes());
			pending = txn.take_pending();
		}

		Ok((pending, all_pending_shapes, all_view_changes))
	}
}
