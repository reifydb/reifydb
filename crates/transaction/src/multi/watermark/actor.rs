// SPDX-License-Identifier: Apache-2.0
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

use reifydb_runtime::{
	actor::{
		context::Context,
		system::ActorConfig,
		traits::{Actor, Directive},
	},
	sync::waiter::WaiterHandle,
};

use super::{MAX_PENDING, MAX_WAITERS, OLD_VERSION_THRESHOLD, PENDING_CLEANUP_THRESHOLD};

const MAX_ORPHANED: usize = 10000;

const ORPHAN_CLEANUP_THRESHOLD: u64 = 1000;

use reifydb_core::actors::watermark::WatermarkMessage;

pub struct WatermarkShared {
	pub done_until: AtomicU64,
	pub last_index: AtomicU64,
}

pub struct WatermarkActor {
	pub shared: Arc<WatermarkShared>,
}

pub struct WatermarkState {
	indices: BinaryHeap<Reverse<u64>>,
	pending: HashMap<u64, i64>,
	begun: HashSet<u64>,
	orphaned_done: HashSet<u64>,
	waiters: HashMap<u64, Vec<Arc<WaiterHandle>>>,
}

impl Actor for WatermarkActor {
	type State = WatermarkState;
	type Message = WatermarkMessage;

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
			WatermarkMessage::Begin {
				version,
			} => {
				state.process_begin(version, &self.shared.done_until);
			}
			WatermarkMessage::Done {
				version,
			} => {
				state.process_done(version, &self.shared.done_until);
			}
			WatermarkMessage::WaitFor {
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
	fn process_begin(&mut self, version: u64, done_until: &AtomicU64) {
		self.cleanup_if_needed(done_until);

		self.begun.insert(version);

		if self.orphaned_done.remove(&version) {
			self.pending.insert(version, 0);
		} else {
			self.pending.entry(version).and_modify(|v| *v += 1).or_insert(1);
		}

		if !self.pending.contains_key(&version) || !self.indices.iter().any(|Reverse(v)| *v == version) {
			self.indices.push(Reverse(version));
		}

		self.try_advance(done_until);
	}

	fn process_done(&mut self, version: u64, done_until: &AtomicU64) {
		self.cleanup_if_needed(done_until);

		if self.begun.contains(&version) {
			self.pending.entry(version).and_modify(|v| *v -= 1).or_insert(-1);
		} else {
			self.orphaned_done.insert(version);
			return;
		}

		self.try_advance(done_until);
	}

	fn try_advance(&mut self, done_until: &AtomicU64) {
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

			self.notify_waiters(old_done_until, until);
		} else {
			let current = done_until.load(Ordering::SeqCst);
			self.waiters.retain(|&idx, waiters_list| {
				if idx <= current {
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

	fn register_waiter(&mut self, version: u64, waiter: Arc<WaiterHandle>, done_until: &AtomicU64) {
		let current = done_until.load(Ordering::SeqCst);
		if current >= version {
			waiter.notify();
		} else if version < current.saturating_sub(OLD_VERSION_THRESHOLD) {
			waiter.notify();
		} else {
			self.waiters.entry(version).or_default().push(waiter);
		}
	}

	fn notify_waiters(&mut self, from: u64, to: u64) {
		(from + 1..=to).for_each(|idx| {
			if let Some(waiters_list) = self.waiters.remove(&idx) {
				for waiter in waiters_list {
					waiter.notify();
				}
			}
		});
	}

	fn cleanup_if_needed(&mut self, done_until: &AtomicU64) {
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
					for waiter in waiters_list.drain(..) {
						waiter.notify();
					}
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
