// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{
	cmp::Reverse,
	collections::{BinaryHeap, HashMap, HashSet},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_runtime::actor::{
	context::Context,
	system::ActorConfig,
	traits::{Actor, Directive},
};

use super::{MAX_PENDING, MAX_WAITERS, OLD_VERSION_THRESHOLD, PENDING_CLEANUP_THRESHOLD, watermark::WaiterHandle};

// Maximum orphaned done() entries before cleanup
const MAX_ORPHANED: usize = 10000;
// Threshold for cleaning up old orphaned entries
const ORPHAN_CLEANUP_THRESHOLD: u64 = 1000;

/// Messages for the watermark actor
#[derive(Debug)]
pub enum WatermarkMsg {
	Begin {
		version: u64,
	},
	Done {
		version: u64,
	},
	WaitFor {
		version: u64,
		waiter: Arc<WaiterHandle>,
	},
}

/// Shared state for fast reads without message passing
pub struct WatermarkShared {
	pub done_until: AtomicU64,
	pub last_index: AtomicU64,
}

/// Watermark actor - tracks minimum unfinished version
pub struct WatermarkActor {
	pub shared: Arc<WatermarkShared>,
}

/// Actor state - owned exclusively by the actor
pub struct WatermarkState {
	indices: BinaryHeap<Reverse<u64>>,
	pending: HashMap<u64, i64>,
	begun: HashSet<u64>,
	orphaned_done: HashSet<u64>,
	waiters: HashMap<u64, Vec<Arc<WaiterHandle>>>,
}

impl Actor for WatermarkActor {
	type State = WatermarkState;
	type Message = WatermarkMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		WatermarkState {
			indices: BinaryHeap::new(),
			pending: HashMap::new(),
			begun: HashSet::new(),
			orphaned_done: HashSet::new(),
			waiters: HashMap::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			WatermarkMsg::Begin {
				version,
			} => {
				state.process_begin(version, &self.shared.done_until);
			}
			WatermarkMsg::Done {
				version,
			} => {
				state.process_done(version, &self.shared.done_until);
			}
			WatermarkMsg::WaitFor {
				version,
				waiter,
			} => {
				state.register_waiter(version, waiter, &self.shared.done_until);
			}
		}
		Directive::Continue
	}

	fn config(&self) -> ActorConfig {
		ActorConfig::new().mailbox_capacity(4096)
	}
}

impl WatermarkState {
	/// Process a begin() call - marks a version as started
	fn process_begin(&mut self, version: u64, done_until: &AtomicU64) {
		self.cleanup_if_needed(done_until);

		// begin() call
		self.begun.insert(version);

		// Check if done() already arrived (orphaned)
		if self.orphaned_done.remove(&version) {
			// Both begin and done have arrived, count is 0
			self.pending.insert(version, 0);
		} else {
			self.pending.entry(version).and_modify(|v| *v += 1).or_insert(1);
		}

		// Add to indices only on begin() - this ensures we track all versions
		if !self.pending.contains_key(&version) || !self.indices.iter().any(|Reverse(v)| *v == version) {
			self.indices.push(Reverse(version));
		}

		self.try_advance(done_until);
	}

	/// Process a done() call - marks a version as completed
	fn process_done(&mut self, version: u64, done_until: &AtomicU64) {
		self.cleanup_if_needed(done_until);

		if self.begun.contains(&version) {
			// Normal case: begin() was called first
			self.pending.entry(version).and_modify(|v| *v -= 1).or_insert(-1);
		} else {
			// Out-of-order: done() arrived before begin()
			// Store it and wait for begin() to arrive
			self.orphaned_done.insert(version);
			return; // Don't advance watermark yet
		}

		self.try_advance(done_until);
	}

	/// Try to advance the watermark
	fn try_advance(&mut self, done_until: &AtomicU64) {
		let old_done_until = done_until.load(Ordering::SeqCst);
		let mut until = old_done_until;

		while !self.indices.is_empty() {
			let min = self.indices.peek().unwrap().0;

			// CRITICAL: Only advance if version was begun (gap-tolerant check)
			if !self.begun.contains(&min) {
				break; // Gap detected - wait for begin()
			}

			if let Some(done) = self.pending.get(&min) {
				if done.gt(&0) {
					break; // Still pending (begin called but not done)
				}
			}
			// Version is complete (begun and done count <= 0)
			self.indices.pop();
			self.pending.remove(&min);
			self.begun.remove(&min);
			until = min;
		}

		if until != old_done_until {
			assert_eq!(
				done_until.compare_exchange(old_done_until, until, Ordering::SeqCst, Ordering::Acquire),
				Ok(old_done_until)
			);

			// Notify all waiters up to the new mark
			self.notify_waiters(old_done_until, until);
		} else {
			// Even if done_until didn't advance, check for any waiters that can be satisfied
			let current = done_until.load(Ordering::SeqCst);
			self.waiters.retain(|&idx, waiters_list| {
				if idx <= current {
					// Signal and remove
					for waiter in waiters_list.drain(..) {
						waiter.notify();
					}
					false
				} else {
					true
				}
			});
		}
	}

	/// Register a waiter for a specific version
	fn register_waiter(&mut self, version: u64, waiter: Arc<WaiterHandle>, done_until: &AtomicU64) {
		let current = done_until.load(Ordering::SeqCst);
		if current >= version {
			// Already done, signal immediately
			waiter.notify();
		} else if version < current.saturating_sub(OLD_VERSION_THRESHOLD) {
			// Version is so old we know it's irrelevant; skip waiter registration
			waiter.notify();
		} else {
			self.waiters.entry(version).or_default().push(waiter);
		}
	}

	/// Notify all waiters between from and to (exclusive of from, inclusive of to)
	fn notify_waiters(&mut self, from: u64, to: u64) {
		(from + 1..=to).for_each(|idx| {
			if let Some(waiters_list) = self.waiters.remove(&idx) {
				// Signal all waiters for this index
				for waiter in waiters_list {
					waiter.notify();
				}
			}
		});
	}

	/// Cleanup old entries to prevent unbounded growth
	fn cleanup_if_needed(&mut self, done_until: &AtomicU64) {
		// Prevent unbounded growth of pending
		if self.pending.len() > MAX_PENDING {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(PENDING_CLEANUP_THRESHOLD);
			self.pending.retain(|&k, _| k > cutoff);
			self.begun.retain(|&k| k > cutoff);
		}

		// Prevent unbounded growth of waiters
		if self.waiters.len() > MAX_WAITERS {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(OLD_VERSION_THRESHOLD);
			self.waiters.retain(|&k, waiters_list| {
				if k <= cutoff {
					// Signal and remove old waiters
					for waiter in waiters_list.drain(..) {
						waiter.notify();
					}
					false
				} else {
					true
				}
			});
		}

		// Prevent unbounded growth of orphaned_done
		if self.orphaned_done.len() > MAX_ORPHANED {
			let current = done_until.load(Ordering::SeqCst);
			let cutoff = current.saturating_sub(ORPHAN_CLEANUP_THRESHOLD);
			self.orphaned_done.retain(|&v| v > cutoff);
		}
	}
}
