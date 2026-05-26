// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, time::Duration};

use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use tokio::{pin, select, sync::watch::Receiver, task::spawn_blocking, time::sleep};

use crate::store::SubscriptionStore;

#[derive(Default)]
struct PollScratch {
	active: Vec<SubscriptionId>,
	drained: Vec<Columns>,
}

pub struct StoreBackedPoller {
	store: Arc<SubscriptionStore>,
	batch_size: usize,
	scratch: Mutex<PollScratch>,
}

impl StoreBackedPoller {
	pub fn new(store: Arc<SubscriptionStore>, batch_size: usize) -> Self {
		Self {
			store,
			batch_size,
			scratch: Mutex::new(PollScratch::default()),
		}
	}

	pub fn poll_all(&self, delivery: &dyn SubscriptionDelivery) -> Option<Duration> {
		let mut scratch = self.scratch.lock();
		let _coord = self.store.begin_poll();

		scratch.active.clear();
		delivery.active_subscriptions_into(&mut scratch.active);

		let PollScratch {
			active,
			drained,
		} = &mut *scratch;
		for sub_id in active.iter() {
			drained.clear();
			self.store.drain_into(sub_id, self.batch_size, drained);
			for columns in drained.drain(..) {
				match delivery.try_deliver(sub_id, columns) {
					DeliveryResult::Delivered => {}
					DeliveryResult::Disconnected => {
						self.store.unregister(sub_id);
						break;
					}
				}
			}
		}

		delivery.flush()
	}

	pub async fn run_loop(self: Arc<Self>, delivery: Arc<dyn SubscriptionDelivery>, mut stop_rx: Receiver<bool>) {
		const NO_DEADLINE: Duration = Duration::from_secs(86_400);
		let mut next_deadline: Option<Duration> = None;
		loop {
			let mut stop = false;
			{
				let notified = self.store.wake().notified();
				pin!(notified);
				select! {
					biased;
					result = stop_rx.changed() => {
						stop = result.is_err() || *stop_rx.borrow();
					}
					_ = &mut notified => {}
					_ = sleep(next_deadline.unwrap_or(NO_DEADLINE)), if next_deadline.is_some() => {}
				}
			}
			if stop {
				break;
			}
			let delivery_ref = delivery.clone();
			let poller = self.clone();
			next_deadline =
				spawn_blocking(move || poller.poll_all(delivery_ref.as_ref())).await.unwrap_or(None);
		}
	}
}
