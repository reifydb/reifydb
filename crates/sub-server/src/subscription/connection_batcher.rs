// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use indexmap::IndexMap;
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::value::duration::Duration;
use tokio::{
	pin, select,
	sync::Notify,
	time::{Instant, sleep},
};

pub type MergeFn<W> = Arc<dyn Fn(&mut W, W) + Send + Sync>;
pub type EmitFn<W> = Arc<dyn Fn(Vec<W>) + Send + Sync>;

struct Pending<W> {
	item: W,
	linger: Duration,
	first_pending_at: Instant,
}

struct Inner<W> {
	pending: Mutex<IndexMap<SubscriptionId, Pending<W>>>,
	merge: MergeFn<W>,
	emit: EmitFn<W>,
	wake: Notify,
}

pub struct ConnectionBatcher<W> {
	inner: Arc<Inner<W>>,
}

impl<W> Clone for ConnectionBatcher<W> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl<W: Send + 'static> ConnectionBatcher<W> {
	pub fn new(merge: MergeFn<W>, emit: EmitFn<W>) -> Self {
		Self {
			inner: Arc::new(Inner {
				pending: Mutex::new(IndexMap::new()),
				merge,
				emit,
				wake: Notify::new(),
			}),
		}
	}

	pub fn append(&self, subscription_id: SubscriptionId, item: W, linger: impl Into<Duration>) {
		let linger = linger.into();
		{
			let mut pending = self.inner.pending.lock();
			match pending.get_mut(&subscription_id) {
				Some(existing) => (self.inner.merge)(&mut existing.item, item),
				None => {
					pending.insert(
						subscription_id,
						Pending {
							item,
							linger,
							first_pending_at: Instant::now(),
						},
					);
				}
			}
		}
		self.inner.wake.notify_one();
	}

	pub fn flush_due(&self) {
		let now = Instant::now();
		self.emit_where(|p| Duration::from_std(now.duration_since(p.first_pending_at)) >= p.linger);
	}

	pub fn flush_all(&self) {
		self.emit_where(|_| true);
	}

	fn emit_where(&self, due: impl Fn(&Pending<W>) -> bool) {
		let items: Vec<W> = {
			let mut pending = self.inner.pending.lock();
			if pending.is_empty() {
				return;
			}
			let keys: Vec<SubscriptionId> =
				pending.iter().filter(|(_, p)| due(p)).map(|(k, _)| *k).collect();
			if keys.is_empty() {
				return;
			}
			keys.into_iter().filter_map(|k| pending.shift_remove(&k).map(|p| p.item)).collect()
		};
		if !items.is_empty() {
			(self.inner.emit)(items);
		}
	}

	fn next_deadline(&self) -> Option<Duration> {
		let now = Instant::now();
		let pending = self.inner.pending.lock();
		pending.values()
			.map(|p| p.linger.saturating_sub(Duration::from_std(now.duration_since(p.first_pending_at))))
			.min()
	}

	pub async fn run(self) {
		loop {
			match self.next_deadline() {
				None => self.inner.wake.notified().await,
				Some(remaining) => {
					let notified = self.inner.wake.notified();
					pin!(notified);
					select! {
						_ = sleep(remaining.to_std()) => {}
						_ = &mut notified => {}
					}
				}
			}
			self.flush_due();
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	use reifydb_core::interface::catalog::id::SubscriptionId;
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_value::value::duration::Duration;
	use tokio::{spawn, time::sleep};

	use super::{ConnectionBatcher, EmitFn, MergeFn};

	fn collecting_emit(out: Arc<Mutex<Vec<Vec<u64>>>>) -> EmitFn<u64> {
		Arc::new(move |items: Vec<u64>| out.lock().push(items))
	}

	fn sum_merge() -> MergeFn<u64> {
		Arc::new(|existing: &mut u64, next: u64| *existing += next)
	}

	fn ms(n: u64) -> Duration {
		Duration::from_milliseconds(n as i64).unwrap()
	}

	#[test]
	fn flush_all_emits_one_batch_in_insertion_order() {
		let out = Arc::new(Mutex::new(Vec::new()));
		let batcher = ConnectionBatcher::new(sum_merge(), collecting_emit(out.clone()));

		batcher.append(SubscriptionId(7), 1, ms(5));
		batcher.append(SubscriptionId(3), 2, ms(5));
		batcher.flush_all();

		let batches = out.lock().clone();
		assert_eq!(batches.len(), 1, "one flush emits exactly one batch");
		assert_eq!(batches[0], vec![1, 2], "distinct subscriptions stay distinct items, in first-seen order");
	}

	#[test]
	fn append_for_same_subscription_merges_into_one_item() {
		let out = Arc::new(Mutex::new(Vec::new()));
		let batcher = ConnectionBatcher::new(sum_merge(), collecting_emit(out.clone()));

		batcher.append(SubscriptionId(7), 10, ms(5));
		batcher.append(SubscriptionId(7), 5, ms(5));
		batcher.append(SubscriptionId(7), 1, ms(5));
		batcher.flush_all();

		let batches = out.lock().clone();
		assert_eq!(
			batches[0],
			vec![16],
			"the same subscription collapses to one merged item, never a second item"
		);
	}

	#[test]
	fn empty_flush_does_not_emit() {
		let out = Arc::new(Mutex::new(Vec::new()));
		let batcher = ConnectionBatcher::new(sum_merge(), collecting_emit(out.clone()));

		batcher.flush_all();

		assert!(out.lock().is_empty(), "a flush with nothing pending must not emit an envelope");
	}

	#[test]
	fn flush_due_holds_a_subscription_until_its_linger_elapses() {
		let out = Arc::new(Mutex::new(Vec::new()));
		let batcher = ConnectionBatcher::new(sum_merge(), collecting_emit(out.clone()));

		batcher.append(SubscriptionId(7), 1, ms(3600_000));
		batcher.flush_due();
		assert!(out.lock().is_empty(), "a subscription is not due until its own linger has elapsed");

		batcher.flush_all();
		assert_eq!(out.lock()[0], vec![1], "flush_all ignores linger (teardown/shape path)");
	}

	#[tokio::test]
	async fn run_loop_respects_per_subscription_linger_independently() {
		let out = Arc::new(Mutex::new(Vec::new()));
		let batcher = ConnectionBatcher::new(sum_merge(), collecting_emit(out.clone()));

		let task = spawn(batcher.clone().run());
		batcher.append(SubscriptionId(1), 1, ms(15));
		batcher.append(SubscriptionId(2), 2, ms(150));

		sleep(ms(60).to_std()).await;
		assert_eq!(
			out.lock().clone(),
			vec![vec![1]],
			"the short-linger subscription is delivered first, in its own envelope, while the long one waits"
		);

		sleep(ms(150).to_std()).await;
		assert_eq!(
			out.lock().clone(),
			vec![vec![1], vec![2]],
			"the long-linger subscription is delivered only after its own linger, in a separate envelope"
		);

		task.abort();
	}

	#[tokio::test]
	async fn run_loop_coalesces_same_linger_subscriptions_into_one_envelope() {
		let out = Arc::new(Mutex::new(Vec::new()));
		let batcher = ConnectionBatcher::new(sum_merge(), collecting_emit(out.clone()));

		let task = spawn(batcher.clone().run());
		batcher.append(SubscriptionId(1), 1, ms(15));
		batcher.append(SubscriptionId(2), 2, ms(15));

		sleep(ms(60).to_std()).await;
		assert_eq!(
			out.lock().clone(),
			vec![vec![1, 2]],
			"subscriptions sharing a linger ride one envelope - the N-into-one case is just equal lingers"
		);

		task.abort();
	}

	#[tokio::test]
	async fn run_loop_flushes_zero_linger_promptly() {
		let out = Arc::new(Mutex::new(Vec::new()));
		let batcher = ConnectionBatcher::new(sum_merge(), collecting_emit(out.clone()));

		let task = spawn(batcher.clone().run());
		batcher.append(SubscriptionId(1), 1, ms(0));

		sleep(ms(30).to_std()).await;
		assert_eq!(
			out.lock().clone(),
			vec![vec![1]],
			"a zero-linger subscription is delivered with no added latency"
		);

		task.abort();
	}

	#[tokio::test]
	async fn run_loop_lets_a_nearer_deadline_preempt_a_pending_sleep() {
		let out = Arc::new(Mutex::new(Vec::new()));
		let batcher = ConnectionBatcher::new(sum_merge(), collecting_emit(out.clone()));

		let task = spawn(batcher.clone().run());
		batcher.append(SubscriptionId(1), 1, ms(150));
		sleep(ms(10).to_std()).await;
		batcher.append(SubscriptionId(2), 2, ms(15));

		sleep(ms(60).to_std()).await;
		assert_eq!(
			out.lock().clone(),
			vec![vec![2]],
			"a later-arriving nearer-deadline subscription preempts the long sleep and flushes first"
		);

		task.abort();
	}

	#[tokio::test]
	async fn run_loop_emits_nothing_while_idle() {
		let calls = Arc::new(AtomicUsize::new(0));
		let calls_for_emit = calls.clone();
		let emit: EmitFn<u64> = Arc::new(move |_| {
			calls_for_emit.fetch_add(1, Ordering::SeqCst);
		});
		let batcher = ConnectionBatcher::new(sum_merge(), emit);

		let task = spawn(batcher.clone().run());
		sleep(ms(40).to_std()).await;
		task.abort();

		assert_eq!(calls.load(Ordering::SeqCst), 0, "an idle connection never wakes to emit");
	}
}
