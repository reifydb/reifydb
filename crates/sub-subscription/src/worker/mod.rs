// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::HashMap,
	panic::{AssertUnwindSafe, catch_unwind},
	process,
	result::Result as StdResult,
	sync::Arc,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			flow::{FlowId, FlowNodeId},
			id::SubscriptionId,
		},
		change::Change,
	},
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, HydrateOutcome, SubscriptionContext},
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::{
	actor::{
		context::Context,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	sync::mutex::Mutex,
};
use reifydb_sub_flow::engine::FlowEngineInner;
use reifydb_transaction::{multi::lease::VersionLeaseGuard, transaction::Transaction};
use reifydb_value::{Result, value::identity::IdentityId};
use tracing::error;

use crate::{sink::DeliveryBuffer, store::SubscriptionStore, subsystem::registration::register_ephemeral_flow};

mod dispatch;
mod hydrate;

pub type SubscriptionEngineFactory = Box<dyn FnOnce() -> FlowEngineInner + Send>;

pub enum SubscriptionWorkerMessage {
	Dispatch {
		to_version: CommitVersion,
		changes: Arc<Vec<Change>>,
		done: Box<dyn FnOnce(Result<()>) + Send>,
	},

	Register {
		flow_id: FlowId,
		flow_dag: FlowDag,
		gate: Option<CommitVersion>,
		ctx: SubscriptionContext,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	},

	Unregister {
		flow_id: FlowId,
		reply: Box<dyn FnOnce() + Send>,
	},

	Hydrate {
		sub_id: SubscriptionId,
		flow_id: FlowId,
		identity: IdentityId,
		lease: VersionLeaseGuard,
		max_rows: u64,
		reply: Box<dyn FnOnce(StdResult<HydrateOutcome, HydrateError>) + Send>,
	},
}

struct SubscriptionFlowState {
	operator_states: HashMap<FlowNodeId, Box<dyn Any + Send>>,
	keyed_state: HashMap<EncodedKey, EncodedRow>,
	gate: Option<CommitVersion>,
}

impl SubscriptionFlowState {
	fn new(gate: Option<CommitVersion>) -> Self {
		Self {
			operator_states: HashMap::new(),
			keyed_state: HashMap::new(),
			gate,
		}
	}
}

pub struct SubscriptionWorkerState {
	flow_engine: FlowEngineInner,
	flows: HashMap<FlowId, SubscriptionFlowState>,
}

pub struct SubscriptionWorkerActor {
	engine: StandardEngine,
	catalog: Catalog,
	store: Arc<SubscriptionStore>,
	delivery: Arc<DeliveryBuffer>,
	engine_factory: Mutex<Option<SubscriptionEngineFactory>>,
}

impl SubscriptionWorkerActor {
	pub fn new<F>(
		engine_factory: F,
		engine: StandardEngine,
		catalog: Catalog,
		store: Arc<SubscriptionStore>,
		delivery: Arc<DeliveryBuffer>,
	) -> Self
	where
		F: FnOnce() -> FlowEngineInner + Send + 'static,
	{
		Self {
			engine,
			catalog,
			store,
			delivery,
			engine_factory: Mutex::new(Some(Box::new(engine_factory))),
		}
	}
}

impl Actor for SubscriptionWorkerActor {
	type State = SubscriptionWorkerState;
	type Message = SubscriptionWorkerMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		let factory = self.engine_factory.lock().take();
		let flow_engine = factory.expect("SubscriptionWorkerActor::init called twice")();
		SubscriptionWorkerState {
			flow_engine,
			flows: HashMap::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		catch_unwind(AssertUnwindSafe(|| {
			match msg {
				SubscriptionWorkerMessage::Dispatch {
					to_version,
					changes,
					done,
				} => {
					let result = self.process_dispatch(state, to_version, &changes);
					if let Err(e) = &result {
						error!(error = %e, "subscription worker dispatch failed");
					}
					done(result);
				}
				SubscriptionWorkerMessage::Register {
					flow_id,
					flow_dag,
					gate,
					ctx,
					reply,
				} => self.handle_register(state, flow_id, flow_dag, gate, ctx, reply),
				SubscriptionWorkerMessage::Unregister {
					flow_id,
					reply,
				} => self.handle_unregister(state, flow_id, reply),
				SubscriptionWorkerMessage::Hydrate {
					sub_id,
					flow_id,
					identity,
					lease,
					max_rows,
					reply,
				} => {
					let outcome =
						self.run_hydrate(state, sub_id, flow_id, identity, lease, max_rows);
					reply(outcome);
				}
			}
			Directive::Continue
		}))
		.unwrap_or_else(|_| {
			error!("panic in subscription worker actor, aborting");
			process::abort()
		})
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::default()
	}
}

impl SubscriptionWorkerActor {
	fn handle_register(
		&self,
		state: &mut SubscriptionWorkerState,
		flow_id: FlowId,
		flow_dag: FlowDag,
		gate: Option<CommitVersion>,
		ctx: SubscriptionContext,
		reply: Box<dyn FnOnce(Result<()>) + Send>,
	) {
		if state.flows.contains_key(&flow_id) {
			reply(Ok(()));
			return;
		}

		let result = self.engine.begin_command(IdentityId::system()).and_then(|mut cmd| {
			let mut txn = Transaction::Command(&mut cmd);
			register_ephemeral_flow(&mut state.flow_engine, &mut txn, flow_dag, &ctx, self.delivery.clone())
		});

		match result {
			Ok(()) => {
				state.flows.insert(flow_id, SubscriptionFlowState::new(gate));
				reply(Ok(()));
			}
			Err(e) => reply(Err(e)),
		}
	}

	fn handle_unregister(
		&self,
		state: &mut SubscriptionWorkerState,
		flow_id: FlowId,
		reply: Box<dyn FnOnce() + Send>,
	) {
		state.flows.remove(&flow_id);
		state.flow_engine.remove_flow(flow_id);
		reply();
	}
}
