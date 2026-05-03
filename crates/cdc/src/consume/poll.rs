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

#[derive(Debug, Clone)]
pub struct PollConsumerConfig {
	pub consumer_id: CdcConsumerId,

	pub thread_name: String,

	pub poll_interval: Duration,

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

pub struct PollConsumer<H: CdcHost, C: CdcConsume + Send + 'static> {
	config: PollConsumerConfig,
	host: Option<H>,
	consumer: Option<C>,
	store: Option<CdcStore>,
	running: Arc<AtomicBool>,
	actor_system: ActorSystem,

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
			return Ok(());
		}

		self.actor_system.shutdown();

		if let Some(handle) = self.handle.take() {
			let _ = handle.join();
		}

		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}
}
