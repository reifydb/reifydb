// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicUsize, Ordering},
};

use reifydb_cdc::consume::consumer::CdcConsume;
use reifydb_core::{
	common::CommitVersion,
	interface::{cdc::Cdc, change::Change},
};
use reifydb_runtime::{actor::mailbox::ActorRef, sync::mutex::Mutex};
use reifydb_value::{Result, error::Error};
use tracing::instrument;

use crate::worker::SubscriptionWorkerMessage;

type Reply = Box<dyn FnOnce(Result<()>) + Send>;

pub struct SubscriptionCdcConsumer {
	workers: Vec<ActorRef<SubscriptionWorkerMessage>>,
}

impl SubscriptionCdcConsumer {
	pub fn new(workers: Vec<ActorRef<SubscriptionWorkerMessage>>) -> Self {
		Self {
			workers,
		}
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

impl CdcConsume for SubscriptionCdcConsumer {
	#[instrument(name = "subscription::consume", level = "debug", skip(self, cdcs, reply), fields(cdc_count = cdcs.len()))]
	fn consume(&self, cdcs: Vec<Cdc>, reply: Reply) {
		if cdcs.is_empty() || self.workers.is_empty() {
			reply(Ok(()));
			return;
		}

		let mut max_version = CommitVersion(0);
		let mut all_changes: Vec<Change> = Vec::new();
		for cdc in cdcs {
			if cdc.version > max_version {
				max_version = cdc.version;
			}
			all_changes.extend(cdc.changes);
		}

		if all_changes.is_empty() {
			reply(Ok(()));
			return;
		}

		let changes = Arc::new(all_changes);
		let barrier = Arc::new(DispatchBarrier {
			remaining: AtomicUsize::new(self.workers.len()),
			reply: Mutex::new(Some(reply)),
			error: Mutex::new(None),
		});

		for worker in &self.workers {
			let barrier_for_done = barrier.clone();
			let done: Box<dyn FnOnce(Result<()>) + Send> =
				Box::new(move |result| barrier_for_done.complete_one(result));
			if worker
				.send(SubscriptionWorkerMessage::Dispatch {
					to_version: max_version,
					changes: changes.clone(),
					done,
				})
				.is_err()
			{
				barrier.complete_one(Ok(()));
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::internal;

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

	// A dispatch failure on any worker (e.g. a residual TXN_012 on a retention-bound breach) must
	// surface as Err so the poll actor reschedules and retries, rather than silently acking the batch
	// and dropping the changes from the subscription. The failing worker here is not the last to
	// complete, so the error must be remembered until the final worker finishes.
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
}
