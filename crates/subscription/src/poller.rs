// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Push-based subscription poller.
//!
//! Polls all registered subscriptions and delivers data via the
//! `SubscriptionDelivery` trait, decoupled from any specific transport.

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{actor::system::ActorSystem, sync::map::Map};
use reifydb_type::Result;
use tracing::{debug, error};

use crate::{
	consumer::SubscriptionConsumer,
	delivery::{DeliveryResult, SubscriptionDelivery},
	state::ConsumptionState,
};

/// Subscription poller that consumes subscription data and delivers via a delivery trait.
pub struct SubscriptionPoller {
	/// Consumption state per subscription
	/// Maps: subscription_id → ConsumptionState
	states: Map<SubscriptionId, ConsumptionState>,
	/// Batch size for reading rows per poll cycle
	batch_size: usize,
}

impl SubscriptionPoller {
	/// Create a new subscription poller.
	///
	/// # Arguments
	///
	/// * `batch_size` - Maximum number of rows to read per subscription per poll cycle
	pub fn new(batch_size: usize) -> Self {
		Self {
			states: Map::new(),
			batch_size,
		}
	}

	/// Register a new subscription for polling.
	///
	/// Should be called when a client subscribes.
	pub fn register(&self, subscription_id: SubscriptionId) {
		self.states.insert(
			subscription_id,
			ConsumptionState {
				db_subscription_id: subscription_id,
				last_consumed_key: None,
			},
		);
		debug!("Registered subscription {} for polling", subscription_id);
	}

	/// Unregister a subscription from polling.
	///
	/// Should be called when a client unsubscribes or disconnects.
	pub fn unregister(&self, subscription_id: &SubscriptionId) {
		self.states.remove(subscription_id);
		debug!("Unregistered subscription {} from polling", subscription_id);
	}

	/// Poll all active subscriptions and deliver data via the delivery trait.
	pub fn poll_all(&self, engine: &StandardEngine, system: &ActorSystem, delivery: &dyn SubscriptionDelivery) {
		let subscription_ids: Vec<_> = self.states.keys();

		for subscription_id in subscription_ids {
			if let Err(e) = self.poll_single(subscription_id, engine, system, delivery) {
				error!("Failed to poll subscription {}: {:?}", subscription_id, e);
			}
		}
	}

	/// Poll a single subscription and deliver data to the client.
	fn poll_single(
		&self,
		subscription_id: SubscriptionId,
		engine: &StandardEngine,
		system: &ActorSystem,
		delivery: &dyn SubscriptionDelivery,
	) -> Result<()> {
		// Get consumption state
		let consumption_state = match self.states.get(&subscription_id) {
			Some(state) => state,
			None => {
				return Ok(());
			}
		};

		let db_subscription_id = consumption_state.db_subscription_id;
		let last_consumed_key = consumption_state.last_consumed_key.clone();
		let batch_size = self.batch_size;
		let engine_clone = engine.clone();

		let read_result = system.install(move || {
			SubscriptionConsumer::read_rows(
				&engine_clone,
				db_subscription_id,
				last_consumed_key.as_ref(),
				batch_size,
			)
		});

		let (rows, row_keys) = read_result?;

		if rows.is_empty() {
			return Ok(());
		}

		// Deliver via the delivery trait
		match delivery.try_deliver(&subscription_id, rows) {
			DeliveryResult::Delivered => {
				// Advance cursor BEFORE deletion for at-least-once guarantee.
				// If delete fails, revert cursor so rows retry next poll.
				let prev_cursor = row_keys.last().and_then(|last_key| {
					self.states.with_write(&subscription_id, |state| {
						let prev = state.last_consumed_key.clone();
						state.last_consumed_key = Some(last_key.clone());
						prev
					})
				});

				let engine_clone = engine.clone();
				let keys_to_delete: Vec<_> = row_keys.clone();

				let delete_result = system.install(move || {
					SubscriptionConsumer::delete_rows(&engine_clone, &keys_to_delete)
				});

				match delete_result {
					Ok(()) => {}
					Err(e) => {
						// Revert cursor on delete failure
						if let Some(prev) = prev_cursor {
							self.states.with_write(&subscription_id, |state| {
								state.last_consumed_key = prev;
							});
						}
						return Err(e);
					}
				}
			}
			DeliveryResult::BackPressure => {
				tracing::warn!("Back pressure for subscription {}, will retry", subscription_id);
			}
			DeliveryResult::Disconnected => {
				debug!("Consumer disconnected for subscription {}, unregistering", subscription_id);
				self.unregister(&subscription_id);
			}
		}

		Ok(())
	}
}
