// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	panic::{AssertUnwindSafe, catch_unwind},
	process,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::catalog::{
	config::{ConfigKey, GetConfig},
	flow::FlowId,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	context::clock::{Clock, Instant},
};
use reifydb_value::value::{datetime::DateTime, duration::Duration, identity::IdentityId};
use tracing::{error, warn};

use crate::{
	engine::FlowEngine,
	transaction::{CommittingParams, FlowTransaction},
};

pub enum TransactionalTickMessage {
	Tick,
}

pub struct TransactionalTickActor {
	flow_engine: FlowEngine,
	engine: StandardEngine,
	catalog: Catalog,
	clock: Clock,
}

impl TransactionalTickActor {
	pub fn new(flow_engine: FlowEngine, engine: StandardEngine, catalog: Catalog, clock: Clock) -> Self {
		Self {
			flow_engine,
			engine,
			catalog,
			clock,
		}
	}
}

pub struct TransactionalTickState {
	last_ticks: HashMap<FlowId, Instant>,
}

impl Actor for TransactionalTickActor {
	type State = TransactionalTickState;
	type Message = TransactionalTickMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.flow_tick(), || TransactionalTickMessage::Tick);
		TransactionalTickState {
			last_ticks: HashMap::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		catch_unwind(AssertUnwindSafe(|| {
			match msg {
				TransactionalTickMessage::Tick => {
					self.run_tick(state);
					ctx.schedule_once(self.flow_tick(), || TransactionalTickMessage::Tick);
				}
			}
			Directive::Continue
		}))
		.unwrap_or_else(|_| {
			error!("panic in transactional flow tick actor, aborting");
			process::abort()
		})
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new()
	}
}

impl TransactionalTickActor {
	fn flow_tick(&self) -> Duration {
		self.catalog.get_config_duration(ConfigKey::FlowTick)
	}

	fn run_tick(&self, state: &mut TransactionalTickState) {
		let now = self.clock.instant();
		let timestamp = match DateTime::from_timestamp_millis(self.clock.now_millis()) {
			Ok(ts) => ts,
			Err(_) => {
				warn!("transactional flow tick: clock millis out of range; skipping");
				return;
			}
		};

		let due_flows = self.collect_due_flows(state, &now);
		if due_flows.is_empty() {
			return;
		}

		for flow_id in due_flows {
			match self.process_one_flow(flow_id, timestamp) {
				Ok(()) => {
					state.last_ticks.insert(flow_id, now.clone());
				}
				Err(reason) => {
					warn!(
						flow_id = flow_id.0,
						reason, "transactional tick failed; will retry on next mailbox wake"
					);
				}
			}
		}
	}

	fn collect_due_flows(&self, state: &TransactionalTickState, now: &Instant) -> Vec<FlowId> {
		let engine = self.flow_engine.read();
		let interval = self.flow_tick();
		let mut due: Vec<FlowId> = Vec::new();
		for (flow_id, flow) in engine.flows.iter() {
			if !flow.ticks() {
				continue;
			}
			let elapsed_due = match state.last_ticks.get(flow_id) {
				Some(last) => now.duration_since(last) >= interval.to_std(),
				None => true,
			};
			if elapsed_due {
				due.push(*flow_id);
			}
		}
		due
	}

	fn process_one_flow(&self, flow_id: FlowId, timestamp: DateTime) -> Result<(), String> {
		let cmd = self.engine.begin_command(IdentityId::system()).map_err(|e| format!("begin_command: {e}"))?;
		let interceptors = self.engine.create_interceptors();

		let mut txn = FlowTransaction::committing(CommittingParams {
			cmd,
			catalog: self.catalog.clone(),
			interceptors,
			clock: self.clock.clone(),
			row_allocators: self.flow_engine.read().row_allocators.clone(),
		})
		.map_err(|e| format!("committing: {e}"))?;

		{
			let engine = self.flow_engine.read();
			engine.process_tick(&mut txn, flow_id, timestamp).map_err(|e| format!("process_tick: {e}"))?;
		}

		txn.flush_operator_states().map_err(|e| format!("flush_operator_states: {e}"))?;

		txn.commit().map(|_| ()).map_err(|e| format!("commit: {e}"))
	}
}
