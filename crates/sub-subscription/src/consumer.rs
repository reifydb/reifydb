// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicUsize, Ordering},
};

use reifydb_cdc::{consume::consumer::CdcConsume, rebuild::rebuild_changes};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		cdc::Cdc,
		change::{Change, ChangeOrigin},
	},
	internal_error,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSpawner},
	sync::mutex::Mutex,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error, value::identity::IdentityId};
use tracing::{instrument, warn};

use crate::{
	delivery::DeliveryBuffer,
	store::SubscriptionStore,
	tracker::{SubscriptionPositionTracker, SubscriptionSourceTracker},
	worker::{SubscriptionWorkerMessage, worker_name},
};

type Reply = Box<dyn FnOnce(Result<()>) + Send>;

pub struct SubscriptionCdcConsumer {
	engine: StandardEngine,
	workers: Vec<ActorRef<SubscriptionWorkerMessage>>,
	source_tracker: SubscriptionSourceTracker,
	position_tracker: SubscriptionPositionTracker,
	store: Arc<SubscriptionStore>,
	delivery: Arc<DeliveryBuffer>,
	in_flight: Mutex<Vec<(usize, Arc<AtomicBool>)>>,
}

impl SubscriptionCdcConsumer {
	pub fn new(
		engine: StandardEngine,
		workers: Vec<ActorRef<SubscriptionWorkerMessage>>,
		source_tracker: SubscriptionSourceTracker,
		position_tracker: SubscriptionPositionTracker,
		store: Arc<SubscriptionStore>,
		delivery: Arc<DeliveryBuffer>,
	) -> Self {
		Self {
			engine,
			workers,
			source_tracker,
			position_tracker,
			store,
			delivery,
			in_flight: Mutex::new(Vec::new()),
		}
	}

	fn rebuild_batch(&self, cdcs: &[Cdc]) -> Result<Vec<Change>> {
		let catalog = self.engine.catalog();
		let mut query = self.engine.begin_query(IdentityId::system())?;
		let mut txn = Transaction::Query(&mut query);

		let mut out: Vec<Change> = Vec::new();
		for cdc in cdcs {
			for change in rebuild_changes(cdc, &catalog, &mut txn)? {
				if let ChangeOrigin::Object(object_id) = &change.origin {
					self.source_tracker.update(*object_id, cdc.version.commit);
				}
				out.push(change);
			}
		}
		Ok(out)
	}
}

struct DispatchBarrier {
	remaining: AtomicUsize,
	reply: Mutex<Option<Reply>>,
	error: Mutex<Option<Error>>,
}

impl DispatchBarrier {
	fn complete_one(&self, result: Result<()>) {
		if let Err(e) = result {
			let mut slot = self.error.lock();
			if slot.is_none() {
				*slot = Some(e);
			}
		}
		if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
			let taken = self.reply.lock().take();
			if let Some(reply) = taken {
				let outcome = self.error.lock().take().map_or(Ok(()), Err);
				reply(outcome);
			}
		}
	}
}

pub struct WorkerDone {
	barrier: Option<Arc<DispatchBarrier>>,
	worker: usize,
	spawner: ActorSpawner,
	outstanding: Arc<AtomicBool>,
}

impl WorkerDone {
	fn new(barrier: Arc<DispatchBarrier>, worker: usize, spawner: ActorSpawner) -> Self {
		Self {
			barrier: Some(barrier),
			worker,
			spawner,
			outstanding: Arc::new(AtomicBool::new(true)),
		}
	}

	fn outstanding(&self) -> (usize, Arc<AtomicBool>) {
		(self.worker, self.outstanding.clone())
	}

	pub fn complete(mut self, result: Result<()>) {
		if let Some(barrier) = self.barrier.take() {
			self.outstanding.store(false, Ordering::Release);
			barrier.complete_one(result);
		}
	}

	fn unsent(mut self) {
		self.abandon("could not be sent its message");
	}

	fn abandon(&mut self, failure: &str) {
		let Some(barrier) = self.barrier.take() else {
			return;
		};
		self.outstanding.store(false, Ordering::Release);
		if self.spawner.cancellation_token().is_none_or(|token| token.is_cancelled()) {
			return;
		}
		barrier.complete_one(Err(internal_error!("{} {}", worker_name(self.worker), failure)));
	}
}

impl Drop for WorkerDone {
	fn drop(&mut self) {
		self.abandon("dropped its done without completing it");
	}
}

impl CdcConsume for SubscriptionCdcConsumer {
	fn overtaken(
		&self,
		cursor: CommitVersion,
		truncated_before: CommitVersion,
		reply: Box<dyn FnOnce(Result<CommitVersion>) + Send>,
	) {
		let subscriptions = self.store.active_subscriptions();
		warn!(
			cursor = cursor.0,
			truncated_before = truncated_before.0,
			terminated = subscriptions.len(),
			"subscription consumer overtaken by retention; terminating all subscriptions, clients must resubscribe"
		);
		for subscription in &subscriptions {
			self.store.unregister(subscription);
		}

		let resume = CommitVersion(truncated_before.0.saturating_sub(1).max(cursor.0));
		let wrapped: Reply = Box::new(move |outcome| reply(outcome.map(|()| resume)));
		let barrier = Arc::new(DispatchBarrier {
			remaining: AtomicUsize::new(self.workers.len().max(1)),
			reply: Mutex::new(Some(wrapped)),
			error: Mutex::new(None),
		});
		if self.workers.is_empty() {
			barrier.complete_one(Ok(()));
			return;
		}
		let spawner = self.engine.spawner();
		let mut in_flight = Vec::with_capacity(self.workers.len());
		for (index, worker) in self.workers.iter().enumerate() {
			let done = WorkerDone::new(barrier.clone(), index, spawner.clone());
			in_flight.push(done.outstanding());
			if let Err(unsent) = worker.send(SubscriptionWorkerMessage::Terminate {
				done,
			}) && let SubscriptionWorkerMessage::Terminate {
				done,
			} = unsent.into_inner()
			{
				done.unsent();
			}
		}
		*self.in_flight.lock() = in_flight;
	}

	#[instrument(name = "subscription::consume", level = "debug", skip(self, cdcs, reply), fields(cdc_count = cdcs.len()))]
	fn consume(&self, cdcs: Vec<Cdc>, reply: Reply) {
		if cdcs.is_empty() || self.workers.is_empty() {
			reply(Ok(()));
			return;
		}

		let mut max_version = CommitVersion(0);
		for cdc in &cdcs {
			if cdc.version.commit > max_version {
				max_version = cdc.version.commit;
			}
		}

		let all_changes = match self.rebuild_batch(&cdcs) {
			Ok(changes) => changes,
			Err(e) => {
				reply(Err(e));
				return;
			}
		};

		if all_changes.is_empty() {
			reply(Ok(()));
			return;
		}

		let changes = Arc::new(all_changes);

		let position_tracker = self.position_tracker.clone();
		let store = self.store.clone();
		let delivery = self.delivery.clone();
		let wrapped_reply: Reply = Box::new(move |outcome| {
			delivery.commit_batch();
			if outcome.is_ok() {
				for subscription_id in store.active_subscriptions() {
					position_tracker.update(subscription_id, max_version);
				}
			}
			reply(outcome);
		});

		let barrier = Arc::new(DispatchBarrier {
			remaining: AtomicUsize::new(self.workers.len()),
			reply: Mutex::new(Some(wrapped_reply)),
			error: Mutex::new(None),
		});

		let spawner = self.engine.spawner();
		let mut in_flight = Vec::with_capacity(self.workers.len());
		for (index, worker) in self.workers.iter().enumerate() {
			let done = WorkerDone::new(barrier.clone(), index, spawner.clone());
			in_flight.push(done.outstanding());
			if let Err(unsent) = worker.send(SubscriptionWorkerMessage::Dispatch {
				to_version: max_version,
				changes: changes.clone(),
				done,
			}) && let SubscriptionWorkerMessage::Dispatch {
				done,
				..
			} = unsent.into_inner()
			{
				done.unsent();
			}
		}
		*self.in_flight.lock() = in_flight;
	}

	fn describe_pending(&self) -> String {
		self.in_flight
			.lock()
			.iter()
			.filter(|(_, outstanding)| outstanding.load(Ordering::Acquire))
			.map(|(worker, _)| worker_name(*worker))
			.collect::<Vec<_>>()
			.join(", ")
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::internal;
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};

	use super::*;

	fn capture() -> (Reply, Arc<Mutex<Option<Result<()>>>>) {
		let slot: Arc<Mutex<Option<Result<()>>>> = Arc::new(Mutex::new(None));
		let slot_for_reply = slot.clone();
		let reply: Reply = Box::new(move |r| *slot_for_reply.lock() = Some(r));
		(reply, slot)
	}

	// A fully successful batch must ack Ok so the CDC poll actor advances its checkpoint past the
	// batch, and the ack must wait for the last worker before firing.
	#[test]
	fn barrier_acks_ok_only_after_all_workers_succeed() {
		let (reply, slot) = capture();
		let barrier = Arc::new(DispatchBarrier {
			remaining: AtomicUsize::new(2),
			reply: Mutex::new(Some(reply)),
			error: Mutex::new(None),
		});

		barrier.complete_one(Ok(()));
		assert!(slot.lock().is_none(), "the batch must not be acked until every worker has completed");

		barrier.complete_one(Ok(()));
		let outcome = slot.lock().take().expect("the reply fires once the last worker completes");
		assert!(outcome.is_ok(), "a fully successful batch must ack Ok so the checkpoint advances");
	}

	// A dispatch failure on any worker must surface as Err so the poll actor reschedules, rather than acking
	// the batch and dropping its changes. The failing worker is not the last to complete, so the error must
	// be remembered until the final one finishes.
	#[test]
	fn barrier_surfaces_err_when_a_worker_fails() {
		let (reply, slot) = capture();
		let barrier = Arc::new(DispatchBarrier {
			remaining: AtomicUsize::new(2),
			reply: Mutex::new(Some(reply)),
			error: Mutex::new(None),
		});

		barrier.complete_one(Err(Error(Box::new(internal!("dispatch failed")))));
		barrier.complete_one(Ok(()));

		let outcome = slot.lock().take().expect("the reply fires once the last worker completes");
		assert!(outcome.is_err(), "a failed worker must fail the whole batch so the consumer retries");
	}

	#[test]
	fn a_worker_that_drops_its_done_fails_the_batch_naming_the_worker() {
		// A done dropped without completing must fail the batch by name, otherwise the barrier waits forever.
		let (reply, slot) = capture();
		let barrier = Arc::new(DispatchBarrier {
			remaining: AtomicUsize::new(2),
			reply: Mutex::new(Some(reply)),
			error: Mutex::new(None),
		});
		let system = ActorSystem::testing(Clock::Real);

		drop(WorkerDone::new(barrier.clone(), 0, system.spawner()));
		WorkerDone::new(barrier.clone(), 1, system.spawner()).complete(Ok(()));

		let outcome = slot.lock().take().expect("the reply must fire once the dropped done is accounted for");
		let error = outcome.expect_err("a batch with a dropped done must not ack Ok");
		assert!(
			error.to_string().contains("subscription-worker-0"),
			"the failure must name the worker that dropped its done: {error}"
		);
	}

	#[test]
	fn a_done_dropped_during_shutdown_neither_replies_nor_panics() {
		// A done dropped once the actor system is cancelled is shutdown and must never fail the batch.
		let (reply, slot) = capture();
		let barrier = Arc::new(DispatchBarrier {
			remaining: AtomicUsize::new(2),
			reply: Mutex::new(Some(reply)),
			error: Mutex::new(None),
		});
		let system = ActorSystem::testing(Clock::Real);
		system.shutdown();

		drop(WorkerDone::new(barrier.clone(), 0, system.spawner()));
		WorkerDone::new(barrier.clone(), 1, system.spawner()).complete(Ok(()));

		assert!(slot.lock().is_none(), "a done dropped during shutdown must not complete the batch");
	}
}
