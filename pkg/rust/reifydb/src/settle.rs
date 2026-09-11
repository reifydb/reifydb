// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Deterministic driver for the subscription pipeline under dst.
//!
//! On a host build the cdc poll consumer, the flow actors and the subscription workers all run on
//! their own threads, so a caller only has to wait. Under dst there are no other threads: the
//! caller is the executor, and nothing moves until it steps the actor system and advances the mock
//! clock. [`Database::settle_subscriptions`] is that driver.

#[cfg(reifydb_dst)]
use std::{cell::Cell, fmt};

use reifydb_cdc::consume::watermark::FlowCaughtUpWatermark;
#[cfg(reifydb_dst)]
use reifydb_core::interface::catalog::config::{ConfigKey, GetConfig};
use reifydb_core::{
	common::CommitVersion, error::diagnostic::subscription::subscription_lagged,
	interface::catalog::id::SubscriptionId, internal,
};
use reifydb_sub_subscription::{store::SubscriptionStore, subsystem::SubscriptionSubsystem};
use reifydb_value::{Result, error::Error, value::duration::Duration};

use crate::Database;

/// How many poll-and-drain passes [`Database::settle_subscriptions_within`] may spend before it
/// gives up. A budget rather than a wall-clock deadline: under dst the only clock that moves is the
/// mock one the loop itself advances, so a time-based bound could never expire.
#[cfg(reifydb_dst)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleBudget {
	/// Enough passes for any pipeline depth a test or an embedded bridge realistically builds.
	Default,
	/// An explicit cap, mostly to prove in a test that the bound reports rather than hangs.
	Passes(u32),
}

#[cfg(reifydb_dst)]
const DEFAULT_PASSES: u32 = 64;

/// The mock clock is advanced by at least this much per pass even if `FLOW_TICK` is configured
/// smaller, so a pathological config cannot turn every pass into a no-op.
#[cfg(reifydb_dst)]
const MIN_STEP: Duration = Duration::from_milliseconds_const(1);

#[cfg(reifydb_dst)]
impl SettleBudget {
	fn passes(self) -> u32 {
		match self {
			Self::Default => DEFAULT_PASSES,
			Self::Passes(n) => n,
		}
	}
}

/// What a completed settle observed. `lagged` is deliberately separate from quiescence: a
/// subscription that overflowed its queue reports no pending batches because the overflow threw the
/// work away, so it settles like any other. Delivery completeness is therefore only claimable when
/// `lagged` is empty.
#[cfg(reifydb_dst)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settled {
	pub passes: u32,
	pub lagged: Vec<SubscriptionId>,
}

#[cfg(reifydb_dst)]
impl Settled {
	pub fn is_complete(&self) -> bool {
		self.lagged.is_empty()
	}
}

/// The four signals the settle loop composes. All must hold at the same sample for the pipeline to
/// be quiescent.
#[cfg(reifydb_dst)]
struct SettleState {
	/// Messages still sitting in the actor system's ready queue.
	actors_pending: bool,
	/// Highest committed version, the version flow output has to reach.
	target: CommitVersion,
	/// How far every deferred flow has materialized. `None` when no flow subsystem published a
	/// watermark, in which case there is nothing to wait for.
	flow_caught_up: Option<CommitVersion>,
	cdc_producer: CommitVersion,
	cdc_consumer: CommitVersion,
	/// Batches staged for a subscriber but not yet drained by one.
	pending_batches: usize,
}

#[cfg(reifydb_dst)]
impl SettleState {
	/// Whether everything committed up to `target` has been materialized and staged.
	fn has_reached(&self, target: CommitVersion) -> bool {
		!self.actors_pending
			&& self.cdc_producer >= target
			&& self.cdc_consumer >= self.cdc_producer
			&& self.flow_caught_up.is_none_or(|flow| flow >= target)
	}

	/// `previous_pending` is the staged-batch count sampled before the last pass, so the fourth
	/// signal reads "the pass produced no new batch".
	///
	/// It deliberately is not `pending_batches == 0`. Nothing inside an embedded database drains
	/// a subscriber's queue; the caller does, after settling. Requiring an empty queue would make
	/// every settle that actually delivered something spin until the budget ran out. `None` means
	/// no pass has run yet, so nothing can be concluded.
	fn is_quiescent(&self, previous_pending: Option<usize>) -> bool {
		!self.actors_pending
			&& self.flow_caught_up.is_none_or(|flow| flow >= self.target)
			&& self.cdc_consumer >= self.cdc_producer
			&& previous_pending == Some(self.pending_batches)
	}
}

#[cfg(reifydb_dst)]
impl fmt::Display for SettleState {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"actors_pending={}, tx_target={}, flow_caught_up={}, cdc_producer={}, cdc_consumer={}, pending_batches={}",
			self.actors_pending,
			self.target.0,
			match self.flow_caught_up {
				Some(v) => v.0.to_string(),
				None => "n/a".to_string(),
			},
			self.cdc_producer.0,
			self.cdc_consumer.0,
			self.pending_batches
		)
	}
}

#[cfg(reifydb_dst)]
thread_local! {
	static SETTLING: Cell<bool> = const { Cell::new(false) };
}

/// Refuses a settle that is nested inside another one.
///
/// Stepping the actor system runs actor handlers inline on this thread. If a handler reaches back
/// into `settle_subscriptions`, the inner `run_until_idle` can re-enter the very cell that is
/// mid-`process_one`, whose state is already mutably borrowed; that panics, and the panic is caught
/// by the executor and turned into a silently dead actor. Failing the nested call loudly is the
/// only way to make that visible from here.
#[cfg(reifydb_dst)]
struct SettleGuard;

#[cfg(reifydb_dst)]
impl SettleGuard {
	fn enter() -> Result<Self> {
		if SETTLING.with(|flag| flag.replace(true)) {
			return Err(Error(Box::new(internal!(
				"settle_subscriptions was re-entered on a thread that is already settling; \
				 it must be called from the thread that drives the actor system, never from \
				 inside an actor handler"
			))));
		}
		Ok(Self)
	}
}

#[cfg(reifydb_dst)]
impl Drop for SettleGuard {
	fn drop(&mut self) {
		SETTLING.with(|flag| flag.set(false));
	}
}

#[cfg(reifydb_dst)]
impl Database {
	/// Advances the subscription pipeline until it stops producing work, then returns.
	///
	/// A single pass cannot be assumed to be enough. A write has to reach the cdc producer, be
	/// polled by the flow actors, be materialized, be polled again by the subscription consumer
	/// and only then be staged on a subscriber's queue; some of those hops are woken eagerly and
	/// some only when a timer fires, so how many passes a write costs depends on the pipeline.
	/// The loop therefore repeats until all four progress signals agree rather than counting hops.
	pub fn settle_subscriptions(&self) -> Result<Settled> {
		self.settle_subscriptions_within(SettleBudget::Default)
	}

	/// [`Self::settle_subscriptions`] with an explicit pass budget. Exceeding the budget is an
	/// error naming every signal that was still outstanding, never a hang.
	pub fn settle_subscriptions_within(&self, budget: SettleBudget) -> Result<Settled> {
		let _guard = SettleGuard::enter()?;

		let Some(subsystem) = self.subsystem::<SubscriptionSubsystem>() else {
			return Ok(Settled {
				passes: 0,
				lagged: Vec::new(),
			});
		};
		let store = subsystem.store().clone();
		let system = self.spawner().system();
		let step = self.settle_step();
		let max_passes = budget.passes();

		system.run_until_idle();

		let mut passes = 0u32;
		let mut previous_pending: Option<usize> = None;
		loop {
			let state = self.sample_settle_state(&store);
			if state.is_quiescent(previous_pending) {
				return Ok(Settled {
					passes,
					lagged: lagged_subscriptions(&store),
				});
			}
			if passes >= max_passes {
				return Err(Error(Box::new(internal!(
					"settle_subscriptions did not reach quiescence within {} passes: {}, \
					 staged_batches_before_last_pass={}",
					max_passes,
					state,
					match previous_pending {
						Some(n) => n.to_string(),
						None => "n/a".to_string(),
					}
				))));
			}
			previous_pending = Some(state.pending_batches);
			passes += 1;

			self.engine().notify_cdc_consumers();
			system.advance_time(step);
			system.run_until_idle();
		}
	}

	/// One flow tick per pass. The subscription consumer polls far more often than that, so a
	/// step this size fires its timer too; a smaller step would spend the whole budget without
	/// ever letting a flow actor tick.
	fn settle_step(&self) -> Duration {
		let tick = self.catalog().get_config_duration(ConfigKey::FlowTick);
		if tick.to_std() < MIN_STEP.to_std() {
			MIN_STEP
		} else {
			tick
		}
	}

	fn sample_settle_state(&self, store: &SubscriptionStore) -> SettleState {
		let engine = self.engine();
		SettleState {
			actors_pending: self.spawner().system().has_pending(),
			target: self.watermarks().tx().current().unwrap_or(CommitVersion(0)),
			flow_caught_up: engine.ioc().try_resolve::<FlowCaughtUpWatermark>().map(|w| w.get()),
			cdc_producer: engine.cdc_producer_watermark(),
			cdc_consumer: engine.cdc_consumer_watermark(),
			pending_batches: store.pending_batches(),
		}
	}
}

/// What a completed [`Database::caught_up`] observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaughtUp {
	pub target: CommitVersion,
	/// Always zero on a threaded build, which waits rather than steps.
	pub passes: u32,
}

/// Wall clock, not the database clock: a seeded build's frozen clock never expires a deadline.
#[cfg(not(reifydb_dst))]
const DEFAULT_WAIT: Duration = Duration::from_milliseconds_const(10_000);

impl Database {
	/// Waits until every change produced by a write that committed before this call has been
	/// materialized and staged for its subscribers. Staging is as far as this goes; delivery
	/// belongs to whoever owns the transport.
	///
	/// The target is snapshotted on entry so a concurrent writer cannot push the frontier away
	/// from a caller trying to reach it. A subscription that overran its queue is refused, because
	/// the overflow threw the work away.
	pub fn caught_up(&self) -> Result<CaughtUp> {
		let Some(subsystem) = self.subsystem::<SubscriptionSubsystem>() else {
			return Ok(CaughtUp {
				target: CommitVersion(0),
				passes: 0,
			});
		};
		let store = subsystem.store().clone();
		let target = self.watermarks().tx().current().unwrap_or(CommitVersion(0));

		let passes = self.reach(target)?;
		refuse_lagged(&store)?;
		Ok(CaughtUp {
			target,
			passes,
		})
	}

	/// Under dst the calling thread runs the actors, the cdc consumer and the flow tick.
	#[cfg(reifydb_dst)]
	fn reach(&self, target: CommitVersion) -> Result<u32> {
		let _guard = SettleGuard::enter()?;
		let system = self.spawner().system();
		let step = self.settle_step();

		system.run_until_idle();

		let mut passes = 0u32;
		loop {
			let state = self.sample_settle_state(
				&self.subsystem::<SubscriptionSubsystem>()
					.expect("caught_up checked for the subsystem before calling reach")
					.store()
					.clone(),
			);
			if state.has_reached(target) {
				return Ok(passes);
			}
			if passes >= DEFAULT_PASSES {
				return Err(Error(Box::new(internal!(
					"caught_up did not reach version {} within {} passes: {}",
					target.0,
					DEFAULT_PASSES,
					state
				))));
			}
			passes += 1;

			self.engine().notify_cdc_consumers();
			system.advance_time(step);
			system.run_until_idle();
		}
	}

	/// The cdc consumer and the flow actors have their own threads, so only the watermarks matter.
	#[cfg(not(reifydb_dst))]
	fn reach(&self, target: CommitVersion) -> Result<u32> {
		let watermarks = self.watermarks();
		let cdc = watermarks.cdc();

		if self.engine().ioc().try_resolve::<FlowCaughtUpWatermark>().is_some()
			&& !cdc.wait_for_flow_consumer(target, DEFAULT_WAIT)
		{
			return Err(Error(Box::new(internal!(
				"caught_up timed out waiting for deferred flows to materialize version {}: \
				 flow_caught_up={}, cdc_producer={}, cdc_consumer={}",
				target.0,
				cdc.flow_consumer().0,
				cdc.producer().0,
				cdc.consumer().0
			))));
		}
		if !cdc.wait_for_consumer(target, DEFAULT_WAIT) {
			return Err(Error(Box::new(internal!(
				"caught_up timed out waiting for the cdc consumer to reach version {}: \
				 cdc_producer={}, cdc_consumer={}",
				target.0,
				cdc.producer().0,
				cdc.consumer().0
			))));
		}
		Ok(0)
	}
}

fn refuse_lagged(store: &SubscriptionStore) -> Result<()> {
	match lagged_subscriptions(store).first() {
		None => Ok(()),
		Some(id) => Err(Error(Box::new(subscription_lagged(
			id.0,
			store.capacity(id),
			store.overrun(id).unwrap_or(0),
		)))),
	}
}

/// Subscriptions whose queue overflowed. Their discarded work is gone for good, so they are quiet
/// but incomplete.
fn lagged_subscriptions(store: &SubscriptionStore) -> Vec<SubscriptionId> {
	let mut lagged: Vec<SubscriptionId> =
		store.active_subscriptions().into_iter().filter(|id| store.overrun(id).is_some()).collect();
	lagged.sort_unstable();
	lagged
}

#[cfg(all(test, reifydb_dst))]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{id::SubscriptionId, subscription::HydrationConfig},
	};
	use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
	use reifydb_value::{params::Params, value::frame::frame::Frame};

	use super::{SettleBudget, SettleState};
	use crate::{Database, WithSubsystem, embedded, subscribe::Subscription};

	fn db() -> Database {
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().seeded(7).fatal(FatalConfig::disarmed()))
			.build()
			.expect("dst database builds")
	}

	fn with_table() -> Database {
		let db = db();
		db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
		db.admin_as_root("CREATE TABLE app::t { id: int4, val: int4 }", Params::None).expect("create table");
		db
	}

	fn subscribe(db: &Database) -> Subscription {
		let sub = db
			.subscribe_as_root("FROM app::t MAP { id, val }", Params::None, HydrationConfig::default())
			.expect("subscribe");
		db.settle_subscriptions().expect("settle after subscribe");
		sub.drain(usize::MAX);
		sub
	}

	fn insert(db: &Database, id: i32, val: i32) {
		db.command_as_root(&format!("INSERT app::t [{{id: {id}, val: {val}}}]"), Params::None).expect("insert");
	}

	fn row_count(sub: &Subscription) -> usize {
		let frames: Vec<Frame> = sub.drain(usize::MAX);
		frames.iter().map(|f| f.row_count()).sum()
	}

	/// A state in which every one of the four signals is satisfied.
	fn quiescent_state() -> SettleState {
		SettleState {
			actors_pending: false,
			target: CommitVersion(10),
			flow_caught_up: Some(CommitVersion(10)),
			cdc_producer: CommitVersion(10),
			cdc_consumer: CommitVersion(10),
			pending_batches: 3,
		}
	}

	#[test]
	fn settle_delivers_a_write_to_a_table_subscription() {
		// The whole point of the driver: under dst the calling thread is the executor, so
		// without it nothing steps the actor system and an inserted row never reaches the
		// subscriber's queue at all.
		let db = with_table();
		let sub = subscribe(&db);

		insert(&db, 1, 10);
		let settled = db.settle_subscriptions().expect("settle after insert");

		assert_eq!(row_count(&sub), 1, "the inserted row must be staged for the subscriber");
		assert!(settled.is_complete(), "no subscription may have overrun: {:?}", settled.lagged);
	}

	#[test]
	fn settle_delivers_every_write_in_a_sequence() {
		// Repeated settling has to keep working, not just the first one: each write must be
		// picked up on the settle that follows it, and none may be left behind for the next.
		let db = with_table();
		let sub = subscribe(&db);

		for id in 1..=3 {
			insert(&db, id, id * 10);
			db.settle_subscriptions().expect("settle after insert");
			assert_eq!(row_count(&sub), 1, "write {id} must be delivered by the settle that follows it");
		}
	}

	#[test]
	fn a_settled_pipeline_stays_settled_for_one_confirming_pass() {
		// Quiescence is "the last pass produced nothing", so one confirming pass is the floor and
		// also the ceiling once the pipeline is quiet. More than that would mean the predicate
		// keeps seeing work that is not there, and every caller would pay for it on every call.
		let db = with_table();
		let sub = subscribe(&db);
		insert(&db, 1, 10);

		let first = db.settle_subscriptions().expect("first settle");
		let second = db.settle_subscriptions().expect("second settle");
		assert_eq!(first.passes, 1, "delivering one write must not need more than the confirming pass");
		assert_eq!(second.passes, 1, "settling an already quiescent pipeline must not spin");
		assert_eq!(row_count(&sub), 1, "the row is still there; settling must not consume it");
	}

	#[test]
	fn exhausting_the_budget_reports_what_was_pending() {
		// A genuine pipeline bug must surface as a diagnosis, not a hang. The same state that a
		// one-pass budget settles must fail under a zero-pass budget, which is what makes this a
		// test of the bound rather than of an unsettleable pipeline.
		let db = with_table();
		let sub = subscribe(&db);
		insert(&db, 1, 10);

		let err = db
			.settle_subscriptions_within(SettleBudget::Passes(0))
			.expect_err("a zero-pass budget cannot confirm quiescence");
		let message = err.to_string();
		assert!(message.contains("did not reach quiescence within 0 passes"), "unexpected message: {message}");
		for signal in [
			"actors_pending=",
			"tx_target=",
			"flow_caught_up=",
			"cdc_producer=",
			"cdc_consumer=",
			"pending_batches=",
			"staged_batches_before_last_pass=",
		] {
			assert!(message.contains(signal), "message must name {signal}, got: {message}");
		}

		db.settle_subscriptions_within(SettleBudget::Passes(1)).expect("one pass is enough for this state");
		assert_eq!(row_count(&sub), 1, "the write is still delivered once the budget allows a pass");
	}

	#[test]
	fn quiescence_needs_all_four_signals() {
		// The four signals answer different questions and none of them subsumes another. A
		// caught-up cdc consumer says nothing about whether a deferred flow materialized its
		// output, and an idle ready queue says nothing about work still parked on a timer. Drop
		// any one clause and this fails.
		let previous = Some(quiescent_state().pending_batches);
		assert!(quiescent_state().is_quiescent(previous), "the all-signals-satisfied state must be quiescent");

		let mut actors = quiescent_state();
		actors.actors_pending = true;
		assert!(!actors.is_quiescent(previous), "messages in the ready queue are outstanding work");

		let mut flow = quiescent_state();
		flow.flow_caught_up = Some(CommitVersion(9));
		assert!(!flow.is_quiescent(previous), "flow output short of the commit target is not materialized yet");

		let mut consumer = quiescent_state();
		consumer.cdc_consumer = CommitVersion(9);
		assert!(!consumer.is_quiescent(previous), "a cdc consumer behind its producer still has work");

		let mut staged = quiescent_state();
		staged.pending_batches += 1;
		assert!(!staged.is_quiescent(previous), "a batch staged during the last pass means work was produced");

		assert!(!quiescent_state().is_quiescent(None), "nothing can be concluded before the first pass");
	}

	#[test]
	fn an_absent_flow_watermark_is_not_treated_as_a_lagging_one() {
		// Without a flow subsystem nobody publishes the materialization watermark. Reading that
		// absence as version zero would make the loop wait forever for output that no one is
		// producing.
		let mut state = quiescent_state();
		state.flow_caught_up = None;
		assert!(state.is_quiescent(Some(state.pending_batches)));
	}

	#[test]
	fn a_nested_settle_is_refused_rather_than_corrupting_an_actor() {
		// Re-entering the driver would let run_until_idle step a cell that is already mid
		// process_one, which panics on the state borrow and is then swallowed by catch_unwind
		// into a silently dead actor. The guard has to turn that into an error the caller sees.
		let db = with_table();
		let outer = super::SettleGuard::enter().expect("first entry");
		let nested = db.settle_subscriptions();
		assert!(nested.is_err(), "a nested settle must be refused");
		assert!(
			nested.unwrap_err().to_string().contains("re-entered"),
			"the refusal must say why it was refused"
		);
		drop(outer);

		db.settle_subscriptions().expect("settling works again once the outer guard is gone");
	}

	#[test]
	fn stopping_settles_before_it_flushes() {
		// stop() drains the cdc consumers and then flushes the stores. Under dst that drain is
		// the only thing between a write and shutdown that can step an actor, so if it does not
		// settle, the write is simply lost at exit. No explicit settle here on purpose.
		let mut db = with_table();
		let sub = subscribe(&db);
		insert(&db, 1, 10);

		db.stop().expect("stop");
		assert_eq!(row_count(&sub), 1, "shutdown must drive the write to the subscriber, not drop it");
	}

	#[test]
	fn a_healthy_subscription_is_not_reported_as_lagged() {
		// Lag and quiescence are deliberately separate: an overrun subscription surrendered its
		// queue and therefore settles like any other, so `lagged` is the only thing that can tell
		// a caller whether delivery was actually complete.
		let db = with_table();
		let sub = subscribe(&db);
		insert(&db, 1, 10);

		let settled = db.settle_subscriptions().expect("settle");
		assert_eq!(settled.lagged, Vec::<SubscriptionId>::new(), "a subscriber that kept up is not lagged");
		assert!(settled.is_complete());
		assert_eq!(row_count(&sub), 1);
	}

	/// Starting the flow subsystem publishes its operator catalog and then waits on the event bus
	/// to drain. That wait used to block the calling thread, which under dst is the only thread
	/// that can run the bus actor, so the reply it waited for could never be sent and `build`
	/// hung forever. Nothing else here can catch that: every other test in this module builds
	/// without a flow subsystem, and a hang is not a failure any assertion can observe.
	#[test]
	fn database_with_flow_subsystem_builds_under_dst() {
		let db = embedded::memory()
			.with_runtime_config(RuntimeConfig::default().seeded(7).fatal(FatalConfig::disarmed()))
			.with_flow(|flow| flow)
			.build()
			.expect("dst database with a flow subsystem builds");

		// Reaching a working database, not merely returning from build, is the point.
		db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	}
}
