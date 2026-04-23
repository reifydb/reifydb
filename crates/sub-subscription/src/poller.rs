// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, time::Duration};

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use tokio::{select, sync::watch::Receiver, task::spawn_blocking, time::interval};

use crate::store::SubscriptionStore;

/// Store-backed subscription poller that drains from in-memory SubscriptionStore
/// instead of polling persistent storage.
pub struct StoreBackedPoller {
	store: Arc<SubscriptionStore>,
	batch_size: usize,
}

impl StoreBackedPoller {
	pub fn new(store: Arc<SubscriptionStore>, batch_size: usize) -> Self {
		Self {
			store,
			batch_size,
		}
	}

	/// Poll all active subscriptions and deliver via the delivery trait.
	///
	/// Holds the store's coord read lock for the full cycle so that commits
	/// from the subscription CDC consumer are blocked until the cycle
	/// completes. This guarantees the poller never observes a partial
	/// commit - either a CDC batch's diffs are all visible, or none are.
	pub fn poll_all(&self, delivery: &dyn SubscriptionDelivery) {
		let _coord = self.store.begin_poll();
		let active = delivery.active_subscriptions();
		for sub_id in active {
			self.poll_single(&sub_id, delivery);
		}
		delivery.flush();
	}

	fn poll_single(&self, sub_id: &SubscriptionId, delivery: &dyn SubscriptionDelivery) {
		let drained = self.store.drain(sub_id, self.batch_size);
		for columns in drained {
			match delivery.try_deliver(sub_id, columns) {
				DeliveryResult::Delivered => {}
				DeliveryResult::Disconnected => {
					self.store.unregister(sub_id);
					break;
				}
			}
		}
	}

	/// Run polling loop with interval-based wakeup.
	pub async fn run_loop(
		self: Arc<Self>,
		delivery: Arc<dyn SubscriptionDelivery>,
		poll_interval: Duration,
		mut stop_rx: Receiver<bool>,
	) {
		let mut interval = interval(poll_interval);
		loop {
			select! {
				biased;
				result = stop_rx.changed() => {
					if result.is_err() || *stop_rx.borrow() {
						break;
					}
				}
				_ = interval.tick() => {
					let delivery_ref = delivery.clone();
					let poller = self.clone();
					let _ = spawn_blocking(move || {
						poller.poll_all(delivery_ref.as_ref());
					}).await;
				}
			}
		}
	}
}
