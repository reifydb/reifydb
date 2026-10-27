// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The napi transport for subscriptions.
//!
//! It carries the same rbcf envelopes the WebSocket transport puts on the wire, so a client decodes
//! a change here with the code it uses against a real server. What differs is only how the bytes
//! travel: the socket pushes them, this hands them to the next call in.

pub mod host;
pub mod sink;

use std::{collections::HashMap, sync::Arc};

use reifydb::{
	Database, Error, IdentityId, Params, Result as ReifyResult,
	core::{
		interface::catalog::{id::SubscriptionId, subscription::SubscribeOptions},
		internal,
	},
	runtime::sync::mutex::Mutex,
	sub_core::{
		handler::{SubscribeError, handle_subscribe, handle_unsubscribe},
		host::SubscribeHost,
		registry::{ConnectionId, SubscriptionRegistry},
	},
	sub_subscription::{poller::StoreBackedPoller, subsystem::SubscriptionSubsystem},
	value::value::uuid::Uuid7,
};
use tokio::{sync::watch, task::JoinHandle};

use crate::subscription::{
	host::NodeSubscribeHost,
	sink::{NodePush, NodeWireSink, Rbcf},
};

/// How many staged batches one poll drains per subscription. Same as the WebSocket server's
/// default, so a subscription that needs several polls there needs the same number here.
const POLL_BATCH_SIZE: usize = 100;

/// How many drain passes one `caught_up` may spend.
const POLL_PASSES: usize = 64;

/// Everything one node instance needs to act as a subscription transport.
///
/// A socket server holds one of these per connection. There is exactly one here: the node process
/// is the connection, so it gets one connection id and one queue for its lifetime.
pub struct Subscriptions {
	connection_id: ConnectionId,
	sink: NodeWireSink,
	registry: Arc<SubscriptionRegistry<NodeWireSink>>,
	poller: Arc<StoreBackedPoller>,
	host: NodeSubscribeHost,
	/// Kept alive so the receivers handed to remote proxies stay open; dropping the sender would
	/// read to them as a shutdown.
	shutdown: watch::Sender<bool>,
	remote_tasks: Mutex<HashMap<SubscriptionId, JoinHandle<()>>>,
}

impl Subscriptions {
	pub fn new(db: &Database) -> ReifyResult<Self> {
		let store = db
			.subsystem::<SubscriptionSubsystem>()
			.ok_or_else(|| Error(Box::new(internal!("the subscription subsystem is not running"))))?
			.store()
			.clone();
		let (shutdown, _) = watch::channel(false);
		let host = NodeSubscribeHost::new(db)?;
		Ok(Self {
			connection_id: Uuid7::generate(host.context().clock(), host.context().rng()),
			sink: NodeWireSink::new(),
			registry: Arc::new(SubscriptionRegistry::new(db.clock().clone())),
			poller: Arc::new(StoreBackedPoller::new(store, POLL_BATCH_SIZE)),
			host,
			shutdown,
			remote_tasks: Mutex::new(HashMap::new()),
		})
	}

	pub async fn subscribe(
		&self,
		identity: IdentityId,
		queries: &[(String, Params, SubscribeOptions)],
	) -> ReifyResult<Result<Vec<SubscriptionId>, SubscribeError<Error>>> {
		let ack = match handle_subscribe(
			&self.host,
			self.connection_id,
			identity,
			queries,
			self.sink.clone(),
			&self.registry,
			Rbcf,
			self.shutdown.subscribe(),
		)
		.await?
		{
			Ok(ack) => ack,
			Err(e) => return Ok(Err(e)),
		};
		self.remote_tasks.lock().extend(ack.remote_handles);
		let mut subscriptions = ack.subscriptions;
		subscriptions.sort_by_key(|entry| entry.index);
		Ok(Ok(subscriptions.into_iter().map(|entry| entry.subscription_id).collect()))
	}

	/// Drops the subscriptions and the rows that back them. An id this node does not hold is skipped,
	/// so unsubscribing twice is not an error.
	pub async fn unsubscribe(&self, subscription_ids: &[SubscriptionId]) -> ReifyResult<()> {
		{
			let mut remote_tasks = self.remote_tasks.lock();
			for subscription_id in subscription_ids {
				if let Some(handle) = remote_tasks.remove(subscription_id) {
					handle.abort();
				}
			}
		}
		handle_unsubscribe(self.host.engine(), &self.registry, self.connection_id, subscription_ids).await?;
		Ok(())
	}

	/// Moves whatever the pipeline staged into the sink, then empties it.
	///
	/// One call is one pass, not a settle: a change that needs another hop is simply not here
	/// yet. The caller loops until a pass comes back empty, which is what makes the delivery
	/// point observable rather than a race.
	pub fn poll(&self) -> Vec<NodePush> {
		self.poller.poll_all(self.registry.as_ref());
		self.sink.drain()
	}

	/// Everything produced by a write that committed before this call.
	///
	/// [`Database::caught_up`] only stages; draining what was staged is this transport's job.
	pub fn caught_up(&self, db: &Database) -> ReifyResult<Vec<NodePush>> {
		db.caught_up()?;

		let mut pushes = Vec::new();
		for _ in 0..POLL_PASSES {
			let batch = self.poll();
			if batch.is_empty() {
				return Ok(pushes);
			}
			pushes.extend(batch);
		}
		Err(Error(Box::new(internal!(
			"the subscription store kept producing batches after {} drain passes, so a caller \
			 that asked to be caught up could never be told it was; a poller that stages work \
			 as fast as it is drained is the only thing that reads like this",
			POLL_PASSES
		))))
	}
}
