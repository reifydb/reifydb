// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::{EventListener, transaction::PostCommitEvent},
	interface::{
		cdc::{Cdc, SystemChange},
		change::Change,
		store::MultiVersionGetPrevious,
	},
	key::{EncodableKey, Key, cdc_exclude::should_exclude_from_cdc, kind::KeyKind, row::RowKey},
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

use crate::{consume::host::CdcHost, storage::CdcStorage};

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
pub struct CdcProducerActor<S, T, H> {
	storage: Arc<S>,
	transaction_store: Arc<T>,
	host: H,
}

impl<S, T, H> CdcProducerActor<S, T, H>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
{
	pub fn new(storage: S, transaction_store: T, host: H) -> Self {
		Self {
			storage: Arc::new(storage),
			transaction_store: Arc::new(transaction_store),
			host,
		}
	}

	fn process(&self, version: CommitVersion, timestamp: u64, deltas: Vec<Delta>) {
		let mut changes: Vec<Change> = Vec::new();
		let mut system_changes: Vec<SystemChange> = Vec::new();
		let registry = self.host.schema_registry();

		trace!(version = version.0, delta_count = deltas.len(), "Processing CDC");

		for delta in deltas {
			let key = delta.key().clone();

			// Skip internal system keys that shouldn't appear in CDC
			if let Some(kind) = Key::kind(&key) {
				if should_exclude_from_cdc(kind) {
					continue;
				}

				// Row deltas → try to decode into columnar Change, fall back to SystemChange
				if kind == KeyKind::Row {
					if let Some(row_key) = RowKey::decode(&key) {
						let decoded = match &delta {
							Delta::Set {
								key,
								values,
							} => {
								let pre = self
									.transaction_store
									.get_previous_version(key, version)
									.ok()
									.flatten();
								if let Some(prev) = pre {
									super::decode::build_update_change(
										registry,
										row_key.primitive,
										row_key.row,
										prev.values,
										values.clone(),
										version,
									)
								} else {
									super::decode::build_insert_change(
										registry,
										row_key.primitive,
										row_key.row,
										values.clone(),
										version,
									)
								}
							}
							Delta::Unset {
								values,
								..
							} => {
								if !values.is_empty() {
									super::decode::build_remove_change(
										registry,
										row_key.primitive,
										row_key.row,
										values.clone(),
										version,
									)
								} else {
									None
								}
							}
							_ => None,
						};

						if let Some(change) = decoded {
							changes.push(change);
							continue;
						}
					}
					// Fall through to SystemChange if decode failed
				}
			}

			// Non-row deltas (or row deltas that failed to decode) → SystemChange
			let change = match delta {
				Delta::Set {
					key,
					values,
				} => {
					let pre = self
						.transaction_store
						.get_previous_version(&key, version)
						.ok()
						.flatten();

					if let Some(prev_values) = pre {
						SystemChange::Update {
							key,
							pre: prev_values.values,
							post: values,
						}
					} else {
						SystemChange::Insert {
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
					SystemChange::Delete {
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

			system_changes.push(change);
		}

		if !changes.is_empty() || !system_changes.is_empty() {
			let cdc = Cdc::new(version, timestamp, changes, system_changes);
			if let Err(e) = self.storage.write(&cdc) {
				error!(version = version.0, "CDC write failed: {:?}", e);
			} else {
				debug!(version = version.0, "CDC written successfully");
			}
		}
	}
}

pub struct CdcProducerState;

impl<S, T, H> Actor for CdcProducerActor<S, T, H>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
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
pub fn spawn_cdc_producer<S, T, H>(
	system: &ActorSystem,
	storage: S,
	transaction_store: T,
	host: H,
) -> ActorHandle<CdcProduceMsg>
where
	S: CdcStorage + Send + Sync + 'static,
	T: MultiVersionGetPrevious + Send + Sync + 'static,
	H: CdcHost,
{
	let actor = CdcProducerActor::new(storage, transaction_store, host);
	system.spawn("cdc-producer", actor)
}
