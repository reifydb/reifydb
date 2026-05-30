// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_core::{
	actors::cdc::{CdcPollHandle, CdcPollMessage},
	interface::cdc::CdcConsumerId,
};
use reifydb_runtime::actor::system::ActorSpawner;
use reifydb_value::Result;

use super::{
	actor::{PollActor, PollActorConfig},
	consumer::{CdcConsume, CdcConsumer},
	host::CdcHost,
	wake::CdcWakeRegistry,
	watermark::CdcConsumerWatermark,
};
use crate::storage::CdcStore;

#[derive(Debug, Clone)]
pub struct PollConsumerConfig {
	pub consumer_id: CdcConsumerId,

	pub thread_name: String,

	pub poll_interval: Duration,

	pub max_batch_size: Option<u64>,

	pub consumer_watermark: Option<CdcConsumerWatermark>,

	pub wake_registry: Option<CdcWakeRegistry>,
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
			consumer_watermark: None,
			wake_registry: None,
		}
	}

	pub fn with_consumer_watermark(mut self, watermark: CdcConsumerWatermark) -> Self {
		self.consumer_watermark = Some(watermark);
		self
	}

	pub fn with_wake_registry(mut self, registry: CdcWakeRegistry) -> Self {
		self.wake_registry = Some(registry);
		self
	}
}

pub struct PollConsumer<H: CdcHost, C: CdcConsume + Send + 'static> {
	config: PollConsumerConfig,
	host: Option<H>,
	consumer: Option<C>,
	store: Option<CdcStore>,
	running: Arc<AtomicBool>,
	spawner: ActorSpawner,

	handle: Option<CdcPollHandle>,
}

impl<H: CdcHost, C: CdcConsume + Send + 'static> PollConsumer<H, C> {
	pub fn new(config: PollConsumerConfig, host: H, consume: C, store: CdcStore, spawner: ActorSpawner) -> Self {
		Self {
			config,
			host: Some(host),
			consumer: Some(consume),
			store: Some(store),
			running: Arc::new(AtomicBool::new(false)),
			spawner,
			handle: None,
		}
	}

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
		let watermark = self.config.consumer_watermark.clone();
		let wake_armed = Arc::new(AtomicBool::new(false));
		let actor =
			PollActor::new(self.build_actor_config(), host, consumer, store, watermark, wake_armed.clone());
		let handle = self.spawner.spawn_system(&self.config.thread_name, actor);
		if let Some(registry) = &self.config.wake_registry {
			registry.register(wake_armed, handle.actor_ref().clone());
		}
		self.handle = Some(handle);
		Ok(())
	}

	fn stop(&mut self) -> Result<()> {
		if !self.running.swap(false, Ordering::AcqRel) {
			return Ok(());
		}

		if let Some(handle) = self.handle.take() {
			let _ = handle.actor_ref().send(CdcPollMessage::Shutdown);
			let _ = handle.join();
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}
}
