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
use reifydb_value::Result;
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
}

impl DispatchBarrier {
	fn complete_one(&self) {
		if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
			let taken = self.reply.lock().take();
			if let Some(reply) = taken {
				reply(Ok(()));
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
		});

		for worker in &self.workers {
			let barrier_for_done = barrier.clone();
			let done: Box<dyn FnOnce() + Send> = Box::new(move || barrier_for_done.complete_one());
			if worker
				.send(SubscriptionWorkerMessage::Dispatch {
					to_version: max_version,
					changes: changes.clone(),
					done,
				})
				.is_err()
			{
				barrier.complete_one();
			}
		}
	}
}
