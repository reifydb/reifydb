// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_core::{actors::cdc::CdcPollHandle, interface::cdc::CdcConsumerId};
use reifydb_runtime::actor::system::ActorSystem;
use reifydb_type::Result;

use super::{
	actor::{PollActor, PollActorConfig},
	consumer::{CdcConsume, CdcConsumer},
	host::CdcHost,
};
use crate::storage::CdcStore;

/// Configuration for a CDC poll consumer
#[derive(Debug, Clone)]
pub struct PollConsumerConfig {
	/// Unique identifier for this consumer
	pub consumer_id: CdcConsumerId,
	/// Thread name for the poll worker
	pub thread_name: String,
	/// How often to poll for new CDC events
	pub poll_interval: Duration,
	/// Maximum batch size for fetching CDC events (None = unbounded)
	pub max_batch_size: Option<u64>,
}

impl PollConsumerConfig {
	pub fn new(
		consumer_id: CdcConsumerId,
		thread_name: impl Into<String>,
		poll_interval: Duration,
		max_batch_size: Option<u64>,
	) -> Self {
		Self {
			consumer_id,
			thread_name: thread_name.into(),
			poll_interval,
			max_batch_size,
		}
	}
}

/// Poll-based CDC consumer backed by an actor.
///
/// Implements the `CdcConsumer` trait for start/stop lifecycle management.
/// Internally uses `PollActor` for the actual polling logic.
pub struct PollConsumer<H: CdcHost, C: CdcConsume + Send + 'static> {
	config: PollConsumerConfig,
	host: Option<H>,
	consumer: Option<C>,
	store: Option<CdcStore>,
	running: Arc<AtomicBool>,
	actor_system: ActorSystem,
	/// Handle to the poll actor - must be joined on stop for proper cleanup
	handle: Option<CdcPollHandle>,
}

impl<H: CdcHost, C: CdcConsume + Send + 'static> PollConsumer<H, C> {
	pub fn new(
		config: PollConsumerConfig,
		host: H,
		consume: C,
		store: CdcStore,
		actor_system: ActorSystem,
	) -> Self {
		Self {
			config,
			host: Some(host),
			consumer: Some(consume),
			store: Some(store),
			running: Arc::new(AtomicBool::new(false)),
			actor_system,
			handle: None,
		}
	}

	/// Take ownership of the host/consumer/store from their `Option` slots.
	/// Panics if called twice; `start`'s `running` swap guards against that.
	fn take_resources(&mut self) -> (H, C, CdcStore) {
		let host = self.host.take().expect("host already consumed");
		let consumer = self.consumer.take().expect("consumer already consumed");
		let store = self.store.take().expect("store already consumed");
		(host, consumer, store)
	}

	fn build_actor_config(&self) -> PollActorConfig {
		PollActorConfig {
			consumer_id: self.config.consumer_id.clone(),
			poll_interval: self.config.poll_interval,
			max_batch_size: self.config.max_batch_size,
		}
	}
}

impl<H: CdcHost, C: CdcConsume + Send + Sync + 'static> CdcConsumer for PollConsumer<H, C> {
	fn start(&mut self) -> Result<()> {
		if self.running.swap(true, Ordering::AcqRel) {
			return Ok(());
		}
		let (host, consumer, store) = self.take_resources();
		let actor = PollActor::new(self.build_actor_config(), host, consumer, store);
		self.handle = Some(self.actor_system.spawn_system(&self.config.thread_name, actor));
		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if !self.running.swap(false, Ordering::AcqRel) {
			return Ok(()); // Already stopped
		}

		// Signal the actor system to shutdown - this will trigger cancellation
		// which the actor checks before each poll
		self.actor_system.shutdown();

		// Join the poll actor thread to ensure proper cleanup
		// This ensures the PollActor (and its consumer)
		// are dropped cleanly before we return
		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}
}
