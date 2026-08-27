// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, VecDeque},
	fmt,
	sync::Arc,
};

use reifydb_core::interface::catalog::id::QueueId;
use reifydb_runtime::sync::{mutex::Mutex, waiter::WaiterHandle};

#[derive(Clone)]
pub struct QueueWakeRegistry(Arc<QueueWakeRegistryInner>);

struct QueueWakeRegistryInner {
	queues: Mutex<HashMap<QueueId, VecDeque<Arc<WaiterHandle>>>>,
}

impl fmt::Debug for QueueWakeRegistry {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("QueueWakeRegistry").finish_non_exhaustive()
	}
}

impl Default for QueueWakeRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl QueueWakeRegistry {
	pub fn new() -> Self {
		Self(Arc::new(QueueWakeRegistryInner {
			queues: Mutex::new(HashMap::new()),
		}))
	}

	pub fn register(&self, queue: QueueId, waiter: Arc<WaiterHandle>) {
		self.0.queues.lock().entry(queue).or_default().push_back(waiter);
	}

	pub fn deregister(&self, queue: QueueId, waiter: &Arc<WaiterHandle>) {
		let mut queues = self.0.queues.lock();
		let Some(waiters) = queues.get_mut(&queue) else {
			return;
		};
		if let Some(index) = waiters.iter().position(|parked| Arc::ptr_eq(parked, waiter)) {
			waiters.remove(index);
		}
		if waiters.is_empty() {
			queues.remove(&queue);
		}
	}

	pub fn nudge(&self, queue: QueueId, count: usize) {
		if count == 0 {
			return;
		}

		let woken = {
			let mut queues = self.0.queues.lock();
			let Some(waiters) = queues.get_mut(&queue) else {
				return;
			};
			let taken: Vec<Arc<WaiterHandle>> = waiters.drain(..count.min(waiters.len())).collect();
			if waiters.is_empty() {
				queues.remove(&queue);
			}
			taken
		};

		for waiter in woken {
			waiter.notify();
		}
	}

	pub fn nudge_all(&self, queue: QueueId) {
		let woken = self.0.queues.lock().remove(&queue).unwrap_or_default();
		for waiter in woken {
			waiter.notify();
		}
	}

	pub fn parked(&self, queue: QueueId) -> usize {
		self.0.queues.lock().get(&queue).map_or(0, VecDeque::len)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::duration::Duration;

	use super::*;

	fn zero() -> Duration {
		Duration::from_milliseconds(0).unwrap()
	}

	fn park(registry: &QueueWakeRegistry, queue: QueueId) -> Arc<WaiterHandle> {
		let waiter = Arc::new(WaiterHandle::new());
		registry.register(queue, waiter.clone());
		waiter
	}

	#[test]
	fn test_a_nudge_wakes_the_oldest_parked_worker_first() {
		// Wake-N FIFO is the whole thundering-herd policy: one committed item must wake exactly
		// one worker, and it must be the one that has been waiting longest. If this silently
		// became LIFO the newest worker starves the oldest, and if it became wake-all every
		// single-item INSERT would spend N-1 workers on an empty rescan.
		let registry = QueueWakeRegistry::new();
		let queue = QueueId(7);

		let first = park(&registry, queue);
		let second = park(&registry, queue);
		let third = park(&registry, queue);

		registry.nudge(queue, 1);

		assert!(first.wait_timeout(zero()), "the oldest parked worker must be the one woken");
		assert!(!second.wait_timeout(zero()), "a single item must not wake a second worker");
		assert!(!third.wait_timeout(zero()));
		assert_eq!(registry.parked(queue), 2, "the woken waiter must leave the park list");
	}

	#[test]
	fn test_a_nudge_that_arrives_before_the_wait_is_not_lost() {
		// The park loop registers BEFORE it scans, precisely so an INSERT committing between the
		// empty scan and the wait cannot be missed. That ordering is only safe if a notify landing
		// on a not-yet-waiting handle is remembered; otherwise the worker sleeps out its whole
		// budget while its item sits ready.
		let registry = QueueWakeRegistry::new();
		let queue = QueueId(1);

		let waiter = park(&registry, queue);
		registry.nudge(queue, 1);

		assert!(waiter.wait_timeout(zero()), "a nudge delivered before the wait must still release it");
	}

	#[test]
	fn test_deregistering_one_waiter_leaves_the_others_parked() {
		// A client that disconnects drops its guard mid-park. Removing the wrong entry would either
		// wake a dead waiter (the nudge is swallowed and a live worker keeps sleeping) or leak the
		// disconnected one forever.
		let registry = QueueWakeRegistry::new();
		let queue = QueueId(3);

		let first = park(&registry, queue);
		let second = park(&registry, queue);

		registry.deregister(queue, &second);
		assert_eq!(registry.parked(queue), 1);

		registry.nudge(queue, 1);

		assert!(first.wait_timeout(zero()), "the surviving waiter must still be reachable");
		assert!(!second.wait_timeout(zero()), "a deregistered waiter must never be notified");
		assert_eq!(registry.parked(queue), 0);
	}

	#[test]
	fn test_nudging_more_items_than_parked_workers_is_harmless() {
		// A batch INSERT of 5 items with 2 idle workers is ordinary. Draining past the end of the
		// list must not panic, and it must leave no empty map entry behind that would grow without
		// bound across queue ids.
		let registry = QueueWakeRegistry::new();
		let queue = QueueId(9);

		let first = park(&registry, queue);
		let second = park(&registry, queue);

		registry.nudge(queue, 5);

		assert!(first.wait_timeout(zero()));
		assert!(second.wait_timeout(zero()));
		assert_eq!(registry.parked(queue), 0);
	}

	#[test]
	fn test_queues_do_not_share_a_park_list() {
		// Every queue has its own workers. A nudge that leaked across queue ids would hand a wake
		// meant for one queue to a worker polling another, which then burns a claim on an empty
		// queue while its own item waits.
		let registry = QueueWakeRegistry::new();

		let mine = park(&registry, QueueId(1));
		let theirs = park(&registry, QueueId(2));

		registry.nudge(QueueId(1), 4);

		assert!(mine.wait_timeout(zero()));
		assert!(!theirs.wait_timeout(zero()), "a nudge must not cross queue boundaries");
		assert_eq!(registry.parked(QueueId(2)), 1);
	}

	#[test]
	fn test_nudging_an_unknown_queue_does_nothing() {
		let registry = QueueWakeRegistry::new();

		registry.nudge(QueueId(42), 3);
		registry.nudge_all(QueueId(42));

		assert_eq!(registry.parked(QueueId(42)), 0);
	}
}
