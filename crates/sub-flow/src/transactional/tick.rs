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

	TickComplete {
		succeeded: Vec<FlowId>,
		at: Instant,
	},
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
	ticking: bool,
}

impl Actor for TransactionalTickActor {
	type State = TransactionalTickState;
	type Message = TransactionalTickMessage;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.flow_tick(), || TransactionalTickMessage::Tick);
		TransactionalTickState {
			last_ticks: HashMap::new(),
			ticking: false,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		catch_unwind(AssertUnwindSafe(|| {
			match msg {
				TransactionalTickMessage::Tick => {
					self.on_tick(state, ctx);
					ctx.schedule_once(self.flow_tick(), || TransactionalTickMessage::Tick);
				}
				TransactionalTickMessage::TickComplete {
					succeeded,
					at,
				} => {
					state.ticking = false;
					for flow_id in succeeded {
						state.last_ticks.insert(flow_id, at.clone());
					}
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

	fn on_tick(&self, state: &mut TransactionalTickState, ctx: &Context<TransactionalTickMessage>) {
		if state.ticking {
			return;
		}

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
		state.ticking = true;

		let engine = self.engine.clone();
		let catalog = self.catalog.clone();
		let clock = self.clock.clone();
		let flow_engine = self.flow_engine.clone();
		let self_ref = ctx.self_ref().clone();

		self.engine.spawner().pools().spawn_task(move || {
			let succeeded = catch_unwind(AssertUnwindSafe(|| {
				let mut succeeded = Vec::new();
				for flow_id in due_flows {
					match commit_tick_flow(
						&engine,
						&catalog,
						&clock,
						&flow_engine,
						flow_id,
						timestamp,
					) {
						Ok(()) => succeeded.push(flow_id),
						Err(reason) => warn!(
							flow_id = flow_id.0,
							reason, "transactional tick failed; will retry next interval"
						),
					}
				}
				succeeded
			}))
			.unwrap_or_else(|_| {
				error!("panic in transactional flow tick actor, aborting");
				process::abort()
			});
			let _ = self_ref.send(TransactionalTickMessage::TickComplete {
				succeeded,
				at: now,
			});
		});
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
}

fn commit_tick_flow(
	engine: &StandardEngine,
	catalog: &Catalog,
	clock: &Clock,
	flow_engine: &FlowEngine,
	flow_id: FlowId,
	timestamp: DateTime,
) -> Result<(), String> {
	let cmd = engine.begin_command(IdentityId::system()).map_err(|e| format!("begin_command: {e}"))?;
	let interceptors = engine.create_interceptors();

	let mut txn = FlowTransaction::committing(CommittingParams {
		cmd,
		catalog: catalog.clone(),
		interceptors,
		clock: clock.clone(),
		allocators: flow_engine.read().allocators.clone(),
	})
	.map_err(|e| format!("committing: {e}"))?;

	{
		let engine = flow_engine.read();
		engine.process_tick(&mut txn, flow_id, timestamp).map_err(|e| format!("process_tick: {e}"))?;
	}

	txn.flush_operator_states().map_err(|e| format!("flush_operator_states: {e}"))?;

	txn.commit().map(|_| ()).map_err(|e| format!("commit: {e}"))
}
