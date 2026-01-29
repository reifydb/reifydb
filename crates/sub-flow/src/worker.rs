// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Flow actor that handles flow processing logic.
//!
//! This module provides an actor-based implementation of flow processing:
//! - [`FlowWorkerActor`]: The actor definition with init/handle methods
//! - [`FlowMsg`]: Messages the actor can receive (Process, Register)
//! - [`FlowResponse`]: Response sent back through callbacks

use std::{mem::take, sync::Mutex};

use reifydb_catalog::catalog::Catalog;
use reifydb_engine::engine::StandardEngine;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	traits::{Actor, Directive},
};
use tracing::{Span, error, instrument};

use crate::{
	FlowEngine,
	instruction::WorkerBatch,
	transaction::{FlowTransaction, pending::PendingWrites},
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
}

/// Response from the flow actor
pub enum FlowResponse {
	/// Operation succeeded with pending writes
	Success(PendingWrites),
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
					Ok(pending) => FlowResponse::Success(pending),
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
					.begin_command()
					.and_then(|mut txn| state.flow_engine.register(&mut txn, flow));

				let resp = match result {
					Ok(_) => FlowResponse::Success(PendingWrites::new()),
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
	#[instrument(name = "flow::actor::process", level = "debug", skip(self, flow_engine, batch), fields(
		instructions = batch.instructions.len(),
		total_changes = tracing::field::Empty
	))]
	fn process_request(
		&self,
		flow_engine: &mut FlowEngine,
		batch: WorkerBatch,
	) -> reifydb_type::Result<PendingWrites> {
		let total_changes: usize = batch.instructions.iter().map(|i| i.changes.len()).sum();
		Span::current().record("total_changes", total_changes);

		let mut pending = PendingWrites::new();

		for instruction in batch.instructions {
			let flow_id = instruction.flow_id;

			if instruction.changes.is_empty() {
				continue;
			}

			let primitive_version = instruction.to_version;

			let primitive_query = self.engine.multi().begin_query_at_version(primitive_version)?;
			let state_query = self.engine.multi().begin_query_at_version(batch.state_version)?;

			let mut txn = FlowTransaction {
				version: primitive_version,
				pending,
				primitive_query,
				state_query,
				catalog: self.catalog.clone(),
			};

			for change in &instruction.changes {
				if let Err(e) = flow_engine.process(&mut txn, change.clone(), flow_id) {
					error!(flow_id = flow_id.0, error = %e, "failed to process flow");
				}
			}

			pending = take(&mut txn.pending);
		}

		Ok(pending)
	}
}
