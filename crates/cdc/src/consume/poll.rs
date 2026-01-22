// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Poll-based CDC consumer that wraps the PollActor with the CdcConsumer interface.

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_core::interface::cdc::CdcConsumerId;
use reifydb_runtime::actor::runtime::{ActorHandle, ActorRuntime};
use reifydb_type::Result;

use super::{
	actor::{PollActor, PollActorConfig, PollMsg},
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
	runtime: ActorRuntime,
	/// Handle to the poll actor - must be joined on stop for proper cleanup
	handle: Option<ActorHandle<PollMsg>>,
}

impl<H: CdcHost, C: CdcConsume + Send + 'static> PollConsumer<H, C> {
	pub fn new(config: PollConsumerConfig, host: H, consume: C, store: CdcStore, runtime: ActorRuntime) -> Self {
		Self {
			config,
			host: Some(host),
			consumer: Some(consume),
			store: Some(store),
			running: Arc::new(AtomicBool::new(false)),
			runtime,
			handle: None,
		}
	}
}

impl<H: CdcHost, C: CdcConsume + Send + 'static> CdcConsumer for PollConsumer<H, C> {
	fn start(&mut self) -> Result<()> {
		if self.running.swap(true, Ordering::AcqRel) {
			return Ok(()); // Already running
		}

		let host = self.host.take().expect("host already consumed");
		let consumer = self.consumer.take().expect("consumer already consumed");
		let store = self.store.take().expect("store already consumed");

		let actor_config = PollActorConfig {
			consumer_id: self.config.consumer_id.clone(),
			poll_interval: self.config.poll_interval,
			max_batch_size: self.config.max_batch_size,
		};

		let actor = PollActor::new(actor_config, host, consumer, store);

		// Use the shared runtime instead of creating a new one
		let handle = self.runtime.spawn(&self.config.thread_name, actor);
		self.handle = Some(handle);

		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if !self.running.swap(false, Ordering::AcqRel) {
			return Ok(()); // Already stopped
		}

		// Signal the runtime to shutdown - this will trigger cancellation
		// which the actor checks before each poll
		self.runtime.shutdown();

		// Join the poll actor thread to ensure proper cleanup
		// This ensures the PollActor (and its consumer, e.g. FlowCoordinator)
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
