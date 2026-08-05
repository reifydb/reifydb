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
use reifydb_flow::transaction::{CommittingParams, FlowTransaction};
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

use crate::engine::FlowEngine;

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
		let timestamp = match DateTime::from_timestamp_millis(self.clock.now().to_millis()) {
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
		substrate: flow_engine.read().substrate.clone(),
		state_budget: flow_engine.read().state_budget.clone(),
	})
	.map_err(|e| format!("committing: {e}"))?;

	let checkpoint = engine.flow_watermark();
	{
		let engine = flow_engine.read();
		engine.process_tick(&mut txn, flow_id, timestamp, checkpoint)
			.map_err(|e| format!("process_tick: {e}"))?;
	}

	txn.flush_operator_states().map_err(|e| format!("flush_operator_states: {e}"))?;

	txn.commit().map(|_| ()).map_err(|e| format!("commit: {e}"))
}

#[cfg(test)]
mod tests {
	use std::{
		ops::Bound,
		sync::Arc,
		thread::sleep,
		time::{Duration as StdDuration, Instant as StdInstant},
	};

	use reifydb_core::{
		actors::pending::{Pending, PendingLayers, PendingWrite},
		interface::{
			WithEventBus,
			catalog::{flow::OperatorId, object::ObjectId},
			change::{Change, ChangeOrigin, Diff},
		},
		state::budget::OperatorStateBudgetHandle,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_flow::transaction::{DeferredParams, TransactionalParams, substrate::FlowSubstrate};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::identity::IdentityId;

	use super::*;
	use crate::{builder::CustomOperators, catalog::FlowCatalog, operator::metrics::OperatorSampleRegistry};

	fn poll<T>(mut probe: impl FnMut() -> Option<T>) -> Option<T> {
		// Std time, not the engine clock: the engine clock is a frozen mock, so an elapsed
		// check against it would hang rather than fail.
		let started = StdInstant::now();
		loop {
			if let Some(found) = probe() {
				return Some(found);
			}
			if started.elapsed() >= StdDuration::from_secs(10) {
				return None;
			}
			sleep(StdDuration::from_millis(10));
		}
	}

	#[test]
	fn a_tick_fired_eviction_is_tracked_on_the_committed_transaction() {
		// Sinks emit only through the FlowTransaction's change accumulator; on the
		// transactional tick path that accumulator used to be dropped when the wrapped
		// CommandTransaction committed, so a timer-driven eviction updated storage without
		// leaving a change record - invisible to CDC and subscription consumers, unlike the
		// identical eviction fired inline during a user commit. Falsified by skipping the
		// accumulator drain in FlowTransaction::commit (the Committing arm): the view row
		// still disappears from storage, but the Remove change record polled for below never
		// appears.
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, v: int4, ts: datetime } with { ts: ts }");
		te.admin("CREATE TRANSACTIONAL RINGBUFFER VIEW app::v { id: int4, v: int4 } \
			 WITH { capacity: 1000, time: event, row: { ttl: { duration: '1s', announce: true } } } \
			 AS { FROM app::t map { id, v } }");
		let engine = te.inner().clone();
		let catalog = engine.catalog();

		let flow_catalog = FlowCatalog::new(catalog.clone());
		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let flows = catalog.list_flows_all(&mut Transaction::Query(&mut query)).expect("list flows");
		let flow_id = flows.first().expect("one flow").id;
		drop(query);

		let flow_engine = FlowEngine::new(
			catalog.clone(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			FlowSubstrate::with_dictionary(engine.dictionary_allocators()),
			OperatorSampleRegistry::new(),
			OperatorStateBudgetHandle::default(),
		);
		let flow = {
			let mut txn = engine.begin_command(IdentityId::system()).expect("command");
			let (flow, _) = flow_catalog
				.get_or_load_flow(&mut Transaction::Command(&mut txn), flow_id)
				.expect("load flow");
			flow_engine.write().register(&mut txn, flow.clone()).expect("register");
			txn.rollback().expect("rollback registration probe");
			flow
		};

		// One row at event time 60s: its 1s ttl timer becomes due at 61s. The batch runs in a
		// Transactional flow transaction exactly like the inline interceptor's scheduler runs
		// it (the Committing variant cannot see its own writes, so the ttl arm - which reads
		// back the expiry index it just wrote - only happens on the pending-based inline
		// path). It arms the timer and leaves the watermark at 60s - too early to fire it.
		te.command(r#"INSERT app::t [{ id: 1, v: 10, ts: "1970-01-01T00:01:00Z" }]"#);
		let table_changes = poll(|| {
			let items = engine
				.cdc_store()
				.read_range(Bound::Unbounded, Bound::Unbounded, 10_000)
				.expect("read cdc range")
				.items;
			let changes: Vec<Change> = items
				.iter()
				.flat_map(|cdc| cdc.changes.clone())
				.filter(|c| matches!(c.origin, ChangeOrigin::Object(ObjectId::Table(_))))
				.collect();
			(!changes.is_empty()).then_some(changes)
		})
		.expect("the insert's cdc record must appear");

		let mut batch_txn = FlowTransaction::transactional(TransactionalParams {
			version: engine.multi().begin_query().expect("query").version(),
			pending: Pending::new(),
			base_pending: Pending::new(),
			query: engine.multi().begin_query().expect("query"),
			state_query: engine.multi().begin_query().expect("state query"),
			single: engine.single().clone(),
			catalog: catalog.clone(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			view_overlay: Arc::new(Vec::new()),
			substrate: flow_engine.read().substrate.clone(),
			state_budget: flow_engine.read().state_budget.clone(),
		});
		flow_engine.read().process_batch(&mut batch_txn, table_changes, flow_id).expect("process batch");
		batch_txn.flush_operator_states().expect("flush batch");
		let batch_pending = batch_txn.take_pending();
		drop(batch_txn);
		let mut batch_cmd = engine.begin_command(IdentityId::system()).expect("command");
		batch_cmd.disable_conflict_tracking().expect("disable conflict tracking");
		for (key, pw) in batch_pending.iter_sorted() {
			match pw {
				PendingWrite::Set(value) => batch_cmd.set(key, value.clone()).expect("set"),
				PendingWrite::Remove {
					announce: true,
				} => batch_cmd.remove(key).expect("remove"),
				PendingWrite::Remove {
					announce: false,
				} => batch_cmd.remove_silent(key).expect("remove silent"),
			}
		}
		batch_cmd.commit_unchecked().expect("commit batch");
		assert_eq!(
			te.query("FROM app::v").first().map(|f| f.row_count()).unwrap_or(0),
			1,
			"precondition: the batch must land the row in the ring before its ttl can evict it"
		);

		// The watermark reaches 120s without a batch to dispatch the timer (restart hydration
		// or a budget-capped dispatch leave exactly this state), so the tick is the only
		// thing that can fire the eviction. The throwaway transaction only carries the
		// hydration read; the advance sticks in the shared substrate.
		let substrate = flow_engine.read().substrate.clone();
		let sources: Vec<OperatorId> = flow
			.get_operator_ids()
			.filter(|id| flow.get_operator(id).is_some_and(|op| op.ty.is_source()))
			.collect();
		assert!(!sources.is_empty(), "the flow under test must have a source to advance");
		let mut probe_txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: engine.current_version().expect("current version"),
			pending: Pending::new(),
			base_pending: PendingLayers::empty(),
			query: engine.multi().begin_query().expect("query"),
			state_query: engine.multi().begin_query().expect("state query"),
			single: engine.single().clone(),
			catalog: catalog.clone(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: substrate.clone(),
			state_budget: OperatorStateBudgetHandle::default(),
		});
		for source in sources {
			substrate.watermarks
				.advance(source, &mut probe_txn, DateTime::from_millis(120_000))
				.expect("advance watermark");
		}
		drop(probe_txn);

		let pre_tick = engine.current_version().expect("current version");
		commit_tick_flow(
			&engine,
			&catalog,
			&engine.clock().clone(),
			&flow_engine,
			flow_id,
			DateTime::from_millis(120_000),
		)
		.expect("commit tick flow");

		assert_eq!(
			te.query("FROM app::v").first().map(|f| f.row_count()).unwrap_or(0),
			0,
			"precondition: the tick must have evicted the expired row from storage; without \
			 the eviction there is no change record whose presence could be asserted"
		);

		let record = poll(|| {
			engine.cdc_store()
				.read_range(Bound::Unbounded, Bound::Unbounded, 10_000)
				.expect("read cdc range")
				.items
				.into_iter()
				.filter(|cdc| cdc.version > pre_tick)
				.flat_map(|cdc| cdc.changes)
				.find(|change| {
					matches!(change.origin, ChangeOrigin::Object(ObjectId::View(_)))
						&& change.diffs.iter().any(|d| {
							matches!(
								d,
								Diff::Remove {
									..
								}
							)
						})
				})
		});
		assert!(
			record.is_some(),
			"a timer-driven eviction committed by commit_tick_flow must land as a tracked \
			 flow change record on the committed transaction, exactly like an inline one"
		);
	}
}
