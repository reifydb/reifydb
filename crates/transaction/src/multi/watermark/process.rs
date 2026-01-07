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
	sync::{Arc, atomic::Ordering},
};

use crossbeam_channel::{Receiver, select};

use super::{MAX_PENDING, MAX_WAITERS, OLD_VERSION_THRESHOLD, PENDING_CLEANUP_THRESHOLD};

// Maximum orphaned done() entries before cleanup
const MAX_ORPHANED: usize = 10000;
// Threshold for cleaning up old orphaned entries
const ORPHAN_CLEANUP_THRESHOLD: u64 = 1000;
use crate::multi::watermark::{
	Closer,
	watermark::{WaiterHandle, WatermarkInner},
};

impl WatermarkInner {
	pub(crate) fn process(&self, rx: Receiver<super::watermark::Mark>, closer: Closer) {
		let mut indices: BinaryHeap<Reverse<u64>> = BinaryHeap::new();
		let mut pending: HashMap<u64, i64> = HashMap::new();
		let mut waiters: HashMap<u64, Vec<Arc<WaiterHandle>>> = HashMap::new();

		// Track begun versions explicitly for gap-tolerant processing
		let mut begun: HashSet<u64> = HashSet::new();
		// Track orphaned done() calls that arrived before begin()
		let mut orphaned_done: HashSet<u64> = HashSet::new();

		let process_one = |idx: u64,
		                   done: bool,
		                   pending: &mut HashMap<u64, i64>,
		                   begun: &mut HashSet<u64>,
		                   orphaned_done: &mut HashSet<u64>,
		                   waiters: &mut HashMap<u64, Vec<Arc<WaiterHandle>>>,
		                   indices: &mut BinaryHeap<Reverse<u64>>| {
			// Prevent unbounded growth
			if pending.len() > MAX_PENDING {
				// Clean up very old pending entries
				let done_until = self.done_until.load(Ordering::SeqCst);
				let cutoff = done_until.saturating_sub(PENDING_CLEANUP_THRESHOLD);
				pending.retain(|&k, _| k > cutoff);
				begun.retain(|&k| k > cutoff);
			}

			if waiters.len() > MAX_WAITERS {
				// Force cleanup of old waiters
				let done_until = self.done_until.load(Ordering::SeqCst);
				let cutoff = done_until.saturating_sub(OLD_VERSION_THRESHOLD);
				waiters.retain(|&k, waiters_list| {
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

			if orphaned_done.len() > MAX_ORPHANED {
				let done_until = self.done_until.load(Ordering::SeqCst);
				let cutoff = done_until.saturating_sub(ORPHAN_CLEANUP_THRESHOLD);
				orphaned_done.retain(|&v| v > cutoff);
			}

			if done {
				if begun.contains(&idx) {
					// Normal case: begin() was called first
					pending.entry(idx).and_modify(|v| *v -= 1).or_insert(-1);
				} else {
					// Out-of-order: done() arrived before begin()
					// Store it and wait for begin() to arrive
					orphaned_done.insert(idx);
					return; // Don't advance watermark yet
				}
			} else {
				// begin() call
				begun.insert(idx);

				// Check if done() already arrived (orphaned)
				if orphaned_done.remove(&idx) {
					// Both begin and done have arrived, count is 0
					pending.insert(idx, 0);
				} else {
					pending.entry(idx).and_modify(|v| *v += 1).or_insert(1);
				}

				// Add to indices only on begin() - this ensures we track all versions
				if !pending.contains_key(&idx) || !indices.iter().any(|Reverse(v)| *v == idx) {
					indices.push(Reverse(idx));
				}
			}

			// Update mark by going through all indices in order;
			// and checking if they have been done. Stop at the
			// first index, which isn't done OR hasn't been begun.
			let done_until = self.done_until.load(Ordering::SeqCst);

			let mut until = done_until;

			while !indices.is_empty() {
				let min = indices.peek().unwrap().0;

				// CRITICAL: Only advance if version was begun (gap-tolerant check)
				if !begun.contains(&min) {
					break; // Gap detected - wait for begin()
				}

				if let Some(done) = pending.get(&min) {
					if done.gt(&0) {
						break; // Still pending (begin called but not done)
					}
				}
				// Version is complete (begun and done count <= 0)
				indices.pop();
				pending.remove(&min);
				begun.remove(&min);
				until = min;
			}

			if until != done_until {
				assert_eq!(
					self.done_until.compare_exchange(
						done_until,
						until,
						Ordering::SeqCst,
						Ordering::Acquire
					),
					Ok(done_until)
				);
			}

			if until != done_until {
				// Notify all waiters up to the new mark
				(done_until + 1..=until).for_each(|idx| {
					if let Some(waiters_list) = waiters.remove(&idx) {
						// Signal all waiters for this index
						for waiter in waiters_list {
							waiter.notify();
						}
					}
				});
			} else {
				// Even if done_until didn't advance, check for
				// any waiters that can be satisfied
				waiters.retain(|&idx, waiters_list| {
					if idx <= self.done_until.load(Ordering::SeqCst) {
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
		};

		loop {
			select! {
				recv(closer.shutdown_rx) -> _ => {
					closer.done();
					return;
				}
				recv(rx) -> mark => {
					match mark {
						Ok(mark) => {
							if let Some(waiter) = mark.waiter {
								let done_until = self.done_until.load(Ordering::SeqCst);
								if done_until >= mark.version {
									// Already done, signal immediately
									waiter.notify();
								} else if mark.version < done_until.saturating_sub(OLD_VERSION_THRESHOLD) {
									// Version is so old we know it's irrelevant; skip waiter registration
									waiter.notify();
								} else {
									waiters.entry(mark.version).or_default().push(waiter);
								}
							} else {
								process_one(mark.version, mark.done, &mut pending, &mut begun, &mut orphaned_done, &mut waiters, &mut indices);
							}
						}
						Err(_) => {
							// Channel closed
							closer.done();
							return;
						}
					}
				}
			}
		}
	}
}
