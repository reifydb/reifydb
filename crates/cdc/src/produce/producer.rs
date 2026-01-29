// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::{EventListener, transaction::PostCommitEvent},
	interface::{
		cdc::{Cdc, CdcChange, CdcSequencedChange},
		store::MultiVersionGetPrevious,
	},
	key::{Key, cdc_exclude::should_exclude_from_cdc},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorConfig, ActorHandle, ActorSystem},
		traits::{Actor, Directive},
	},
	clock::Clock,
};
use tracing::{debug, error, trace};

use crate::storage::CdcStorage;

/// Message type for the CDC producer actor.
#[derive(Clone, Debug)]
pub struct CdcProduceMsg {
	pub version: CommitVersion,
	pub timestamp: u64,
	pub deltas: Vec<Delta>,
}

/// Actor that processes CDC work items.
///
/// Receives commit data and generates CDC entries, writing them to storage.
/// Uses the shared ActorRuntime, so it works in both native and WASM.
pub struct CdcProducerActor<S, T> {
	storage: Arc<S>,
	transaction_store: Arc<T>,
}

impl<S, T> CdcProducerActor<S, T>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
{
	pub fn new(storage: S, transaction_store: T) -> Self {
		Self {
			storage: Arc::new(storage),
			transaction_store: Arc::new(transaction_store),
		}
	}

	fn process(&self, version: CommitVersion, timestamp: u64, deltas: Vec<Delta>) {
		let mut changes = Vec::new();
		let mut seq = 0u16;

		trace!(version = version.0, delta_count = deltas.len(), "Processing CDC");

		for delta in deltas {
			let key = delta.key().clone();

			// Skip internal system keys that shouldn't appear in CDC
			if let Some(kind) = Key::kind(&key) {
				if should_exclude_from_cdc(kind) {
					continue;
				}
			}

			seq += 1;

			let change = match delta {
				Delta::Set {
					key,
					values,
				} => {
					// Check if previous version exists to determine Insert vs Update
					let pre = self
						.transaction_store
						.get_previous_version(&key, version)
						.ok()
						.flatten();

					if let Some(prev_values) = pre {
						CdcChange::Update {
							key,
							pre: prev_values.values,
							post: values,
						}
					} else {
						CdcChange::Insert {
							key,
							post: values,
						}
					}
				}
				Delta::Unset {
					key,
					values,
				} => {
					let pre = if values.is_empty() {
						None
					} else {
						Some(values)
					};
					CdcChange::Delete {
						key,
						pre,
					}
				}
				Delta::Remove {
					..
				}
				| Delta::Drop {
					..
				} => {
					continue;
				}
			};

			changes.push(CdcSequencedChange {
				sequence: seq,
				change,
			});
		}

		if !changes.is_empty() {
			let cdc = Cdc::new(version, timestamp, changes);
			if let Err(e) = self.storage.write(&cdc) {
				error!(version = version.0, "CDC write failed: {:?}", e);
			} else {
				debug!(version = version.0, "CDC written successfully");
			}
		}
	}
}

pub struct CdcProducerState;

impl<S, T> Actor for CdcProducerActor<S, T>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
{
	type State = CdcProducerState;
	type Message = CdcProduceMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		debug!("CDC producer actor started");
		CdcProducerState
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		if ctx.is_cancelled() {
			debug!("CDC producer actor stopping");
			return Directive::Stop;
		}

		self.process(msg.version, msg.timestamp, msg.deltas);
		Directive::Continue
	}

	fn post_stop(&self) {
		debug!("CDC producer actor stopped");
	}

	fn config(&self) -> ActorConfig {
		// Use a larger mailbox for CDC events which can come in bursts
		ActorConfig::new().mailbox_capacity(256)
	}
}

/// Event listener that forwards PostCommitEvent to the CDC producer actor.
pub struct CdcProducerEventListener {
	actor_ref: ActorRef<CdcProduceMsg>,
	clock: Clock,
}

impl CdcProducerEventListener {
	pub fn new(actor_ref: ActorRef<CdcProduceMsg>, clock: Clock) -> Self {
		Self {
			actor_ref,
			clock,
		}
	}
}

impl EventListener<PostCommitEvent> for CdcProducerEventListener {
	fn on(&self, event: &PostCommitEvent) {
		let msg = CdcProduceMsg {
			version: *event.version(),
			timestamp: self.clock.now_millis(),
			deltas: event.deltas().iter().cloned().collect(),
		};

		if let Err(e) = self.actor_ref.send(msg) {
			error!("Failed to send CDC event to producer actor: {:?}", e);
		}
	}
}

/// Spawn a CDC producer actor on the given actor system.
///
/// Returns a handle to the actor. The actor_ref from this handle should be used
/// to create a `CdcProducerEventListener` which is then registered on the EventBus.
pub fn spawn_cdc_producer<S, T>(system: &ActorSystem, storage: S, transaction_store: T) -> ActorHandle<CdcProduceMsg>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
{
	let actor = CdcProducerActor::new(storage, transaction_store);
	system.spawn("cdc-producer", actor)
}
