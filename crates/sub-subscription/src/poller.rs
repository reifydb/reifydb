// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::subscription::subscription_lagged,
	interface::{catalog::id::SubscriptionId, change::StagedBatch},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use reifydb_value::{reifydb_assertions, value::duration::Duration};
use tokio::{
	pin, select,
	sync::{Notify, watch::Receiver},
	task::spawn_blocking,
	time::sleep,
};
use tracing::instrument;

use crate::store::SubscriptionStore;

#[derive(Default)]
struct PollScratch {
	active: Vec<SubscriptionId>,
	drained: Vec<StagedBatch>,
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

	#[instrument(name = "subscription::poll", level = "debug", skip_all)]
	pub fn poll_all(&self, delivery: &dyn SubscriptionDelivery) -> Option<Duration> {
		let mut scratch = self.scratch.lock();
		let _coord = self.store.begin_poll();

		self.reset_active(&mut scratch, delivery);

		let PollScratch {
			active,
			drained,
		} = &mut *scratch;
		for sub_id in active.iter() {
			if self.store.is_hydrating(sub_id) {
				continue;
			}
			if let Some(overrun) = self.store.overrun(sub_id) {
				delivery.terminate(
					sub_id,
					subscription_lagged(sub_id.0, self.store.capacity(sub_id), overrun),
				);
				self.store.unregister(sub_id);
				continue;
			}
			drained.clear();
			self.store.drain_into(sub_id, self.batch_size, drained);
			for (op, columns) in drained.drain(..) {
				match delivery.try_deliver(sub_id, op, columns) {
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

	#[inline]
	fn reset_active(&self, scratch: &mut PollScratch, delivery: &dyn SubscriptionDelivery) {
		scratch.active.clear();
		reifydb_assertions! {
			let stale = scratch.active.len();
			assert!(
				stale == 0,
				"poll scratch.active must be emptied before repopulation; a leftover id from the previous \
				 poll would be delivered to again this cycle, including ids unregistered since (stale len={stale})"
			);
		}
		delivery.active_subscriptions_into(&mut scratch.active);
	}

	pub async fn run_loop(self: Arc<Self>, delivery: Arc<dyn SubscriptionDelivery>, mut stop_rx: Receiver<bool>) {
		let no_deadline = Duration::from_seconds(86_400).unwrap();
		let simulated_clock = delivery.simulated_clock();
		let wake = self.register_wakers(delivery.as_ref());
		let mut next_deadline: Option<Duration> = None;
		loop {
			let mut stop = false;
			let sleep_for = Self::sleep_for(next_deadline, no_deadline, simulated_clock);
			{
				let notified = wake.notified();
				pin!(notified);
				select! {
					biased;
					result = stop_rx.changed() => {
						stop = result.is_err() || *stop_rx.borrow();
					}
					_ = &mut notified => {}
					_ = sleep(sleep_for.to_std()), if next_deadline.is_some() => {}
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

	const SIMULATED_CLOCK_POLL_MS: i64 = 5;

	fn sleep_for(next_deadline: Option<Duration>, no_deadline: Duration, simulated_clock: bool) -> Duration {
		match next_deadline {
			Some(deadline) if simulated_clock => {
				deadline.min(Duration::from_milliseconds(Self::SIMULATED_CLOCK_POLL_MS).unwrap())
			}
			Some(deadline) => deadline,
			None => no_deadline,
		}
	}

	#[inline]
	fn register_wakers(&self, delivery: &dyn SubscriptionDelivery) -> Arc<Notify> {
		let wake = Arc::new(Notify::new());
		self.store.register_waker(wake.clone());
		delivery.register_waker(wake.clone());
		wake
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		sync::atomic::{AtomicBool, Ordering},
	};

	use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
	use reifydb_value::{
		error::Diagnostic,
		fragment::Fragment,
		value::{Value, diff_type::DiffType},
	};

	use super::*;
	use crate::store::HydrationGuard;

	struct RecordingDelivery {
		active: Mutex<Vec<SubscriptionId>>,
		delivered: Mutex<Vec<(SubscriptionId, u8)>>,
		terminated: Mutex<Vec<SubscriptionId>>,
		disconnected: AtomicBool,
	}

	impl RecordingDelivery {
		fn with_active(ids: &[SubscriptionId]) -> Self {
			Self {
				active: Mutex::new(ids.to_vec()),
				delivered: Mutex::new(Vec::new()),
				terminated: Mutex::new(Vec::new()),
				disconnected: AtomicBool::new(false),
			}
		}

		fn values(&self) -> Vec<u8> {
			self.delivered.lock().iter().map(|(_, value)| *value).collect()
		}
	}

	impl SubscriptionDelivery for RecordingDelivery {
		fn try_deliver(
			&self,
			subscription: &SubscriptionId,
			_op: DiffType,
			columns: Columns,
		) -> DeliveryResult {
			if self.disconnected.load(Ordering::SeqCst) {
				return DeliveryResult::Disconnected;
			}
			self.delivered.lock().push((*subscription, first_value(&columns)));
			DeliveryResult::Delivered
		}

		fn active_subscriptions(&self) -> Vec<SubscriptionId> {
			self.active.lock().clone()
		}

		fn terminate(&self, subscription: &SubscriptionId, _diagnostic: Diagnostic) {
			self.terminated.lock().push(*subscription);
		}
	}

	fn columns(value: u8) -> Columns {
		Columns::new(vec![ColumnWithName::new(Fragment::internal("test"), ColumnBuffer::uint1(vec![value]))])
	}

	fn first_value(columns: &Columns) -> u8 {
		match columns.iter().next().expect("one column").data().get_value(0) {
			Value::Uint1(value) => value,
			other => panic!("expected Uint1, got {other:?}"),
		}
	}

	fn stage(id: SubscriptionId, values: &[u8]) -> HashMap<SubscriptionId, Vec<StagedBatch>> {
		let mut staged = HashMap::new();
		staged.insert(id, values.iter().copied().map(|value| (DiffType::Insert, columns(value))).collect());
		staged
	}

	fn fixture(capacity: usize, batch_size: usize) -> (Arc<SubscriptionStore>, SubscriptionId, StoreBackedPoller) {
		let store = Arc::new(SubscriptionStore::new(capacity));
		let id = store.next_id();
		store.register(id);
		let poller = StoreBackedPoller::new(store.clone(), batch_size);
		(store, id, poller)
	}

	#[test]
	fn poll_all_delivers_every_staged_batch_in_order() {
		// Nothing else drains the store, so a batch that reaches it and is not polled reaches no
		// subscriber at all. Order matters too: a subscriber fed shuffled changes reconstructs the
		// wrong row state.
		let (store, id, poller) = fixture(16, 100);
		let delivery = RecordingDelivery::with_active(&[id]);

		store.commit_staged(stage(id, &[1, 2, 3]));
		poller.poll_all(&delivery);

		assert_eq!(delivery.values(), vec![1, 2, 3], "every staged batch must be delivered, in order");
		assert_eq!(store.pending_batches(), 0, "a delivered batch must leave the queue");
	}

	#[test]
	fn poll_all_skips_a_hydrating_subscription_until_hydration_ends() {
		// The hydrating mark must suppress delivery only while it is held. A mark that outlives
		// hydration silences the subscription for the life of the process, and its queue grows until
		// it overruns.
		let (store, id, poller) = fixture(16, 100);
		let delivery = RecordingDelivery::with_active(&[id]);

		let guard = HydrationGuard::new(&store, id);
		store.commit_staged(stage(id, &[1]));
		poller.poll_all(&delivery);
		assert!(delivery.values().is_empty(), "a hydrating subscription must not be delivered to");
		assert_eq!(store.pending_batches(), 1, "its batch must be held, not dropped");

		drop(guard);
		poller.poll_all(&delivery);
		assert_eq!(delivery.values(), vec![1], "once hydration ends the held batch must be delivered");
	}

	#[test]
	fn poll_all_terminates_an_overrun_subscription_and_stops_tracking_it() {
		// An overrun subscription has already lost data. It must be told and dropped, never left on a
		// live connection quietly receiving a gap-ridden stream.
		let (store, id, poller) = fixture(2, 100);
		let delivery = RecordingDelivery::with_active(&[id]);

		store.commit_staged(stage(id, &[1]));
		store.commit_staged(stage(id, &[2]));
		store.commit_staged(stage(id, &[3]));
		assert!(store.overrun(&id).is_some(), "the fixture must actually have overrun");

		poller.poll_all(&delivery);

		assert_eq!(delivery.terminated.lock().clone(), vec![id], "an overrun subscription must be terminated");
		assert!(!store.contains(&id), "a terminated subscription must be unregistered");
		assert!(delivery.values().is_empty(), "no partial batch may be delivered after an overrun");
	}

	#[test]
	fn poll_all_unregisters_a_subscription_whose_sink_disconnected() {
		// A disconnected sink never returns, so its queue must stop being fed; otherwise every later
		// commit stages batches for a subscriber that no longer exists.
		let (store, id, poller) = fixture(16, 100);
		let delivery = RecordingDelivery::with_active(&[id]);
		delivery.disconnected.store(true, Ordering::SeqCst);

		store.commit_staged(stage(id, &[1]));
		poller.poll_all(&delivery);

		assert!(!store.contains(&id), "a disconnected subscription must be unregistered");
	}

	#[test]
	fn poll_all_delivers_no_more_than_the_batch_size_per_cycle() {
		// The batch size bounds how long one poll holds the store's coordination guard; a poll that
		// ignored it would stall every writer behind a large backlog.
		let (store, id, poller) = fixture(16, 2);
		let delivery = RecordingDelivery::with_active(&[id]);

		store.commit_staged(stage(id, &[1, 2, 3, 4, 5]));

		poller.poll_all(&delivery);
		assert_eq!(delivery.values(), vec![1, 2], "one cycle delivers at most the batch size");
		assert_eq!(store.pending_batches(), 3, "the remainder must stay queued");

		poller.poll_all(&delivery);
		assert_eq!(delivery.values(), vec![1, 2, 3, 4], "the next cycle continues where it left off");
	}
}
