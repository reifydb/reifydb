// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BinaryHeap, HashMap, HashSet},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_runtime::sync::waiter::WaiterHandle;

use super::{MAX_PENDING, MAX_WAITERS, OLD_VERSION_THRESHOLD, PENDING_CLEANUP_THRESHOLD};

const MAX_ORPHANED: usize = 10000;

const ORPHAN_CLEANUP_THRESHOLD: u64 = 1000;

pub struct WatermarkShared {
	pub done_until: AtomicU64,
	pub last_index: AtomicU64,
}

#[derive(Default)]
pub struct WatermarkState {
	indices: BinaryHeap<Reverse<u64>>,
	pending: HashMap<u64, i64>,
	begun: HashSet<u64>,
	orphaned_done: HashSet<u64>,
	waiters: HashMap<u64, Vec<Arc<WaiterHandle>>>,
}

impl WatermarkState {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn process_begin(&mut self, version: u64, done_until: &AtomicU64, out: &mut Vec<Arc<WaiterHandle>>) {
		self.cleanup_if_needed(done_until, out);

		self.begun.insert(version);

		if self.orphaned_done.remove(&version) {
			self.pending.insert(version, 0);
		} else {
			self.pending.entry(version).and_modify(|v| *v += 1).or_insert(1);
		}

		if !self.indices.iter().any(|Reverse(v)| *v == version) {
			self.indices.push(Reverse(version));
		}

		self.try_advance(done_until, out);
	}

	pub fn process_done(&mut self, version: u64, done_until: &AtomicU64, out: &mut Vec<Arc<WaiterHandle>>) {
		self.cleanup_if_needed(done_until, out);

		if self.begun.contains(&version) {
			self.pending.entry(version).and_modify(|v| *v -= 1).or_insert(-1);
		} else {
			self.orphaned_done.insert(version);
			return;
		}

		self.try_advance(done_until, out);
	}

	pub fn register_waiter(
		&mut self,
		version: u64,
		waiter: Arc<WaiterHandle>,
		done_until: &AtomicU64,
		out: &mut Vec<Arc<WaiterHandle>>,
	) {
		let current = done_until.load(Ordering::SeqCst);
		if current >= version || version < current.saturating_sub(OLD_VERSION_THRESHOLD) {
			out.push(waiter);
		} else {
			self.waiters.entry(version).or_default().push(waiter);
		}
	}

	fn try_advance(&mut self, done_until: &AtomicU64, out: &mut Vec<Arc<WaiterHandle>>) {
		let old_done_until = done_until.load(Ordering::SeqCst);
		let mut until = old_done_until;

		while !self.indices.is_empty() {
			let min = self.indices.peek().unwrap().0;

			if !self.begun.contains(&min) {
				break;
			}

			if let Some(done) = self.pending.get(&min)
				&& done.gt(&0)
			{
				break;
			}

			self.indices.pop();
			self.pending.remove(&min);
			self.begun.remove(&min);
			until = min;
		}

		if until != old_done_until {
			done_until.fetch_max(until, Ordering::SeqCst);

			self.notify_waiters(old_done_until, until, out);
		} else {
			let current = done_until.load(Ordering::SeqCst);
			self.waiters.retain(|&idx, waiters_list| {
				if idx <= current {
					out.append(waiters_list);
					false
				} else {
					true
				}
			});
		}
	}

	fn notify_waiters(&mut self, from: u64, to: u64, out: &mut Vec<Arc<WaiterHandle>>) {
		(from + 1..=to).for_each(|idx| {
			if let Some(mut waiters_list) = self.waiters.remove(&idx) {
				out.append(&mut waiters_list);
			}
		});
	}

	fn cleanup_if_needed(&mut self, done_until: &AtomicU64, out: &mut Vec<Arc<WaiterHandle>>) {
		if self.pending.len() > MAX_PENDING {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(PENDING_CLEANUP_THRESHOLD);
			self.pending.retain(|&k, _| k > cutoff);
			self.begun.retain(|&k| k > cutoff);
		}

		if self.waiters.len() > MAX_WAITERS {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(OLD_VERSION_THRESHOLD);
			self.waiters.retain(|&k, waiters_list| {
				if k <= cutoff {
					out.append(waiters_list);
					false
				} else {
					true
				}
			});
		}

		if self.orphaned_done.len() > MAX_ORPHANED {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(ORPHAN_CLEANUP_THRESHOLD);
			self.orphaned_done.retain(|&v| v > cutoff);
		}
	}
}
