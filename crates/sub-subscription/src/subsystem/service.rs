// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	result::Result as StdResult,
	sync::{Arc, mpsc},
};

use reifydb_core::interface::catalog::{flow::FlowId, id::SubscriptionId};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, HydrateOutcome, SubscriptionContext, SubscriptionService},
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSpawner},
	sync::rwlock::RwLock,
};
use reifydb_transaction::{
	error::TransactionError,
	multi::{lease::VersionLeaseGuard, transaction::MultiTransaction},
	transaction::Transaction,
};
use reifydb_value::{Result, value::identity::IdentityId};

use crate::{
	store::SubscriptionStore,
	tracker::SubscriptionPositionTracker,
	worker::{SubscriptionWorkerMessage, worker_name},
};

pub(super) struct SubscriptionState {
	pub(super) store: Arc<SubscriptionStore>,
	pub(super) workers: Vec<ActorRef<SubscriptionWorkerMessage>>,
	pub(super) subscription_flows: RwLock<HashMap<SubscriptionId, FlowId>>,
	pub(super) multi: MultiTransaction,
	pub(super) position_tracker: SubscriptionPositionTracker,
	pub(super) spawner: ActorSpawner,
}

#[cfg(reifydb_dst)]
fn await_reply<T>(spawner: &ActorSpawner, rx: &mpsc::Receiver<T>) -> Option<T> {
	if spawner.is_alive() {
		spawner.system().run_until_idle();
	}
	rx.try_recv().ok()
}

#[cfg(not(reifydb_dst))]
fn await_reply<T>(_spawner: &ActorSpawner, rx: &mpsc::Receiver<T>) -> Option<T> {
	rx.recv().ok()
}

impl SubscriptionState {
	fn worker_index(&self, flow_id: FlowId) -> usize {
		(flow_id.0 as usize) % self.workers.len()
	}

	fn worker_for(&self, flow_id: FlowId) -> &ActorRef<SubscriptionWorkerMessage> {
		&self.workers[self.worker_index(flow_id)]
	}

	#[track_caller]
	fn expect_shutdown(&self, op: &str, flow_id: FlowId, id: SubscriptionId, failure: &str) {
		if self.spawner.cancellation_token().is_none_or(|token| token.is_cancelled()) {
			return;
		}
		panic!(
			"subscription {} of {} failed: {} {}",
			op,
			id.0,
			worker_name(self.worker_index(flow_id)),
			failure
		);
	}
}

pub(super) struct SubscriptionServiceImpl {
	pub(super) state: Arc<SubscriptionState>,
}

impl SubscriptionServiceImpl {
	fn resolve_flow_id(&self, sub_id: SubscriptionId) -> StdResult<FlowId, HydrateError> {
		self.state.subscription_flows.read().get(&sub_id).copied().ok_or(HydrateError::SubscriptionNotFound)
	}

	fn register_with_worker(&self, flow_dag: FlowDag, ctx: SubscriptionContext) -> Result<()> {
		let current = self.state.multi.begin_query()?.version();
		self.state.position_tracker.update(ctx.id, current);

		let id = ctx.id;
		let flow_id = flow_dag.id;

		let (tx, rx) = mpsc::channel();
		let reply: Box<dyn FnOnce(Result<()>) + Send> = Box::new(move |r| {
			let _ = tx.send(r);
		});
		if self.state
			.worker_for(flow_id)
			.send(SubscriptionWorkerMessage::Register {
				flow_id,
				flow_dag,
				ctx,
				reply,
			})
			.is_err()
		{
			self.state.expect_shutdown("register", flow_id, id, "could not be sent its message");
			return Err(TransactionError::ShuttingDown.into());
		}
		let Some(result) = await_reply(&self.state.spawner, &rx) else {
			self.state.expect_shutdown("register", flow_id, id, "dropped its reply");
			return Err(TransactionError::ShuttingDown.into());
		};
		result
	}
}

impl SubscriptionService for SubscriptionServiceImpl {
	fn next_id(&self) -> SubscriptionId {
		self.state.store.next_id()
	}

	fn register_subscription(
		&self,
		flow_dag: FlowDag,
		_hydration_enabled: bool,
		ctx: SubscriptionContext,
		_txn: &mut Transaction<'_>,
	) -> Result<()> {
		let id = ctx.id;
		let flow_id = flow_dag.id;
		self.state.store.register(id);

		self.register_with_worker(flow_dag, ctx).inspect_err(|_| {
			self.state.store.unregister(&id);
			self.state.position_tracker.remove(&id);
		})?;

		self.state.subscription_flows.write().insert(id, flow_id);
		Ok(())
	}

	fn unregister_subscription(&self, id: &SubscriptionId) -> Result<bool> {
		let existed = self.state.store.unregister(id);
		self.state.position_tracker.remove(id);

		if let Some(flow_id) = self.state.subscription_flows.write().remove(id) {
			let (tx, rx) = mpsc::channel();
			let reply: Box<dyn FnOnce() + Send> = Box::new(move || {
				let _ = tx.send(());
			});
			if self.state
				.worker_for(flow_id)
				.send(SubscriptionWorkerMessage::Unregister {
					flow_id,
					reply,
				})
				.is_err()
			{
				self.state.expect_shutdown("unregister", flow_id, *id, "could not be sent its message");
				return Ok(existed);
			}
			if await_reply(&self.state.spawner, &rx).is_none() {
				self.state.expect_shutdown("unregister", flow_id, *id, "dropped its reply");
			}
		}

		Ok(existed)
	}

	fn hydrate(
		&self,
		sub_id: SubscriptionId,
		_engine: &StandardEngine,
		identity: IdentityId,
		lease: VersionLeaseGuard,
		max_rows: u64,
	) -> StdResult<HydrateOutcome, HydrateError> {
		let flow_id = self.resolve_flow_id(sub_id)?;

		let (tx, rx) = mpsc::channel();
		let reply: Box<dyn FnOnce(StdResult<HydrateOutcome, HydrateError>) + Send> = Box::new(move |r| {
			let _ = tx.send(r);
		});
		if self.state
			.worker_for(flow_id)
			.send(SubscriptionWorkerMessage::Hydrate {
				sub_id,
				flow_id,
				identity,
				lease,
				max_rows,
				reply,
			})
			.is_err()
		{
			self.state.expect_shutdown("hydrate", flow_id, sub_id, "could not be sent its message");
			return Err(HydrateError::Engine(TransactionError::ShuttingDown.into()));
		}
		let Some(result) = await_reply(&self.state.spawner, &rx) else {
			self.state.expect_shutdown("hydrate", flow_id, sub_id, "dropped its reply");
			return Err(HydrateError::Engine(TransactionError::ShuttingDown.into()));
		};
		result
	}
}

#[cfg(test)]
mod tests {
	use std::{
		fmt::Debug,
		panic::{AssertUnwindSafe, catch_unwind},
		thread,
	};

	use reifydb_runtime::{
		actor::{
			context::Context,
			mailbox::{ActorRef, SendError},
			system::{ActorHandle, ActorSystem},
			traits::{Actor, Directive},
		},
		context::clock::Clock,
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{params::Params, value::duration::Duration};

	use super::*;

	const SUBSCRIPTION: SubscriptionId = SubscriptionId(4242);
	const WORKER: &str = "subscription-worker-0";

	struct ReplyDroppingWorker;

	impl Actor for ReplyDroppingWorker {
		type State = ();
		type Message = SubscriptionWorkerMessage;

		fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

		fn handle(
			&self,
			_state: &mut Self::State,
			msg: Self::Message,
			_ctx: &Context<Self::Message>,
		) -> Directive {
			drop(msg);
			Directive::Continue
		}
	}

	struct Fixture {
		service: SubscriptionServiceImpl,
		worker: ActorHandle<SubscriptionWorkerMessage>,
	}

	fn fixture(t: &TestEngine, spawner: ActorSpawner) -> Fixture {
		let worker = spawner.spawn_coordination("reply-dropping-worker", ReplyDroppingWorker);
		let state = Arc::new(SubscriptionState {
			store: Arc::new(SubscriptionStore::new(16)),
			workers: vec![worker.actor_ref().clone()],
			subscription_flows: RwLock::new(HashMap::new()),
			multi: t.inner().multi_owned(),
			position_tracker: SubscriptionPositionTracker::new(),
			spawner,
		});
		Fixture {
			service: SubscriptionServiceImpl {
				state,
			},
			worker,
		}
	}

	fn track(fixture: &Fixture, id: SubscriptionId) {
		fixture.service.state.store.register(id);
		fixture.service.state.subscription_flows.write().insert(id, FlowId(id.0));
	}

	fn probe() -> SubscriptionWorkerMessage {
		SubscriptionWorkerMessage::Unregister {
			flow_id: FlowId(0),
			reply: Box::new(|| {}),
		}
	}

	fn wait_until_closed(worker: &ActorRef<SubscriptionWorkerMessage>) {
		for _ in 0..2000 {
			match worker.send(probe()) {
				Err(SendError::Closed(_)) => return,
				_ => thread::sleep(Duration::from_milliseconds(5).unwrap().to_std()),
			}
		}
		panic!("the shut down worker's mailbox never closed");
	}

	fn shut_down_service(t: &TestEngine, system: &ActorSystem) -> SubscriptionServiceImpl {
		let Fixture {
			service,
			worker,
		} = fixture(t, system.spawner());
		system.shutdown();
		worker.join().expect("the worker must finish once the actor system shuts down");
		wait_until_closed(&service.state.workers[0]);
		service
	}

	fn context() -> SubscriptionContext {
		SubscriptionContext {
			id: SUBSCRIPTION,
			identity: IdentityId::root(),
			symbols: Default::default(),
			params: Params::None,
		}
	}

	fn panic_message<T: Debug>(call: impl FnOnce() -> T) -> String {
		match catch_unwind(AssertUnwindSafe(call)) {
			Ok(returned) => panic!("expected a panic, but the call returned {:?}", returned),
			Err(payload) => payload
				.downcast_ref::<String>()
				.cloned()
				.or_else(|| payload.downcast_ref::<&str>().map(|message| message.to_string()))
				.expect("the panic must carry a message"),
		}
	}

	fn assert_names_op_worker_and_subscription(message: &str, op: &str) {
		assert!(message.contains(op), "the panic must name the {} op: {}", op, message);
		assert!(message.contains(WORKER), "the panic must name the worker: {}", message);
		assert!(
			message.contains(&SUBSCRIPTION.0.to_string()),
			"the panic must name the subscription: {}",
			message
		);
	}

	#[test]
	fn unregister_panics_naming_the_worker_when_the_reply_is_dropped() {
		// A worker that drops an unregister reply outside shutdown is a bug and must stop the process by name.
		let t = TestEngine::new();
		let fixture = fixture(&t, t.inner().spawner());
		track(&fixture, SUBSCRIPTION);

		let message = panic_message(|| fixture.service.unregister_subscription(&SUBSCRIPTION));

		assert_names_op_worker_and_subscription(&message, "unregister");
	}

	#[test]
	fn hydrate_panics_naming_the_worker_when_the_reply_is_dropped() {
		// A worker that drops a hydrate reply outside shutdown is a bug and must stop the process by name.
		let t = TestEngine::new();
		let fixture = fixture(&t, t.inner().spawner());
		track(&fixture, SUBSCRIPTION);
		let version = t.inner().current_version().expect("current version");
		let lease = t.inner().acquire_version_lease(version).expect("lease the current version");

		let message = panic_message(|| {
			fixture.service.hydrate(SUBSCRIPTION, t.inner(), IdentityId::root(), lease, 16)
		});

		assert_names_op_worker_and_subscription(&message, "hydrate");
	}

	#[test]
	fn register_panics_naming_the_worker_when_the_reply_is_dropped() {
		// A worker that drops a register reply outside shutdown is a bug and must stop the process by name.
		let t = TestEngine::new();
		let fixture = fixture(&t, t.inner().spawner());
		let mut query = t.inner().begin_query(IdentityId::system()).expect("begin query");
		let ctx = SubscriptionContext {
			id: SUBSCRIPTION,
			identity: IdentityId::root(),
			symbols: Default::default(),
			params: Params::None,
		};

		let message = panic_message(|| {
			fixture.service.register_subscription(
				FlowDag::builder(FlowId(SUBSCRIPTION.0)).build(),
				false,
				ctx,
				&mut Transaction::Query(&mut query),
			)
		});

		assert_names_op_worker_and_subscription(&message, "register");
		assert!(
			!message.contains("unregister"),
			"a register failure must not be reported as unregister: {}",
			message
		);
	}

	#[test]
	fn unregister_during_shutdown_still_reports_whether_the_subscription_existed() {
		// A worker lost to shutdown is expected, so unregister must answer instead of panicking.
		let t = TestEngine::new();
		let system = ActorSystem::testing(Clock::Real);
		let fixture = fixture(&t, system.spawner());
		track(&fixture, SUBSCRIPTION);
		let worker = fixture.service.state.workers[0].clone();
		system.shutdown();
		fixture.worker.join().expect("the worker must finish once the actor system shuts down");
		wait_until_closed(&worker);

		let existed = fixture.service.unregister_subscription(&SUBSCRIPTION);

		assert!(
			matches!(existed, Ok(true)),
			"unregister during shutdown must return Ok(true) for a tracked subscription, got {:?}",
			existed
		);
	}

	#[test]
	fn register_during_shutdown_fails_as_shutting_down_and_leaves_nothing_behind() {
		// A register lost to shutdown must fail as shutting down and roll back, or its id leaks in the store.
		let t = TestEngine::new();
		let system = ActorSystem::testing(Clock::Real);
		let service = shut_down_service(&t, &system);
		let mut query = t.inner().begin_query(IdentityId::system()).expect("begin query");

		let result = service.register_subscription(
			FlowDag::builder(FlowId(SUBSCRIPTION.0)).build(),
			false,
			context(),
			&mut Transaction::Query(&mut query),
		);

		let error = result.expect_err("register during shutdown must not succeed");
		assert!(
			TransactionError::is_shutting_down(&error),
			"register during shutdown must fail as shutting down, got {:?}",
			error
		);
		assert!(
			!service.state.store.contains(&SUBSCRIPTION),
			"a register lost to shutdown must leave no store entry"
		);
		assert!(
			!service.state.position_tracker.all().contains_key(&SUBSCRIPTION),
			"a register lost to shutdown must leave no tracked position"
		);
		assert!(
			!service.state.subscription_flows.read().contains_key(&SUBSCRIPTION),
			"a register lost to shutdown must leave no flow mapping"
		);
	}

	#[test]
	fn hydrate_during_shutdown_fails_as_shutting_down() {
		// A hydrate lost to shutdown must fail as shutting down, never as an internal error.
		let t = TestEngine::new();
		let system = ActorSystem::testing(Clock::Real);
		let service = shut_down_service(&t, &system);
		service.state.store.register(SUBSCRIPTION);
		service.state.subscription_flows.write().insert(SUBSCRIPTION, FlowId(SUBSCRIPTION.0));
		let version = t.inner().current_version().expect("current version");
		let lease = t.inner().acquire_version_lease(version).expect("lease the current version");

		match service.hydrate(SUBSCRIPTION, t.inner(), IdentityId::root(), lease, 16) {
			Err(HydrateError::Engine(error)) if TransactionError::is_shutting_down(&error) => {}
			other => panic!("hydrate during shutdown must fail as shutting down, got {:?}", other),
		}
	}
}
