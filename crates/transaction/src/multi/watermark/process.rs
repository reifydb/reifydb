// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{
	cmp::Reverse,
	collections::{BinaryHeap, HashMap},
	sync::atomic::Ordering,
};

use mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, oneshot};

use super::{MAX_PENDING, MAX_WAITERS, OLD_VERSION_THRESHOLD, PENDING_CLEANUP_THRESHOLD};
use crate::multi::watermark::{Closer, watermark::WatermarkInner};

impl WatermarkInner {
	pub(crate) async fn process(
		&self,
		mut rx: UnboundedReceiver<super::watermark::Mark>,
		closer: Closer,
		mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
	) {
		let mut indices: BinaryHeap<Reverse<u64>> = BinaryHeap::new();
		let mut pending: HashMap<u64, i64> = HashMap::new();
		let mut waiters: HashMap<u64, Vec<oneshot::Sender<()>>> = HashMap::new();

		let process_one = |idx: u64,
		                   done: bool,
		                   pending: &mut HashMap<u64, i64>,
		                   waiters: &mut HashMap<u64, Vec<oneshot::Sender<()>>>,
		                   indices: &mut BinaryHeap<Reverse<u64>>| {
			// Prevent unbounded growth
			if pending.len() > MAX_PENDING {
				// Clean up very old pending entries
				let done_until = self.done_until.load(Ordering::SeqCst);
				let cutoff = done_until.saturating_sub(PENDING_CLEANUP_THRESHOLD);
				pending.retain(|&k, _| k > cutoff);
			}

			if waiters.len() > MAX_WAITERS {
				// Force cleanup of old waiters
				let done_until = self.done_until.load(Ordering::SeqCst);
				let cutoff = done_until.saturating_sub(OLD_VERSION_THRESHOLD);
				waiters.retain(|&k, waiters_list| {
					if k <= cutoff {
						// Signal and remove old waiters
						for waiter in waiters_list.drain(..) {
							let _ = waiter.send(());
						}
						false
					} else {
						true
					}
				});
			}

			if !pending.contains_key(&idx) {
				indices.push(Reverse(idx));
			}

			let mut delta = 1;
			if done {
				delta = -1;
			}
			pending.entry(idx).and_modify(|v| *v += delta).or_insert(delta);

			// Update mark by going through all indices in order;
			// and checking if they have been done. Stop at the
			// first index, which isn't done.
			let done_until = self.done_until.load(Ordering::SeqCst);

			// Marks can arrive out of order due to concurrent sends to the channel.
			// If we skip late-arriving begin() marks, those versions will never be
			// tracked and the watermark will incorrectly skip them.
			// We must process ALL marks to ensure watermark advancement is correct.

			let mut until = done_until;

			while !indices.is_empty() {
				let min = indices.peek().unwrap().0;

				if let Some(done) = pending.get(&min) {
					if done.gt(&0) {
						break; // len(indices) will be > 0.
					}
				}
				// Even if done is called multiple times causing
				// it to become negative, we should still pop the index.
				indices.pop();
				pending.remove(&min);
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
							let _ = waiter.send(());
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
							let _ = waiter.send(());
						}
						false
					} else {
						true
					}
				});
			}
		};

		loop {
			tokio::select! {
				_ = shutdown_rx.recv() => {
					closer.done();
					return;
				}
				mark = rx.recv() => {
					match mark {
						Some(mark) => {
							if let Some(wait_tx) = mark.waiter {
								let done_until = self.done_until.load(Ordering::SeqCst);
								if done_until >= mark.version {
									// Already done, signal immediately
									let _ = wait_tx.send(());
								} else if mark.version < done_until.saturating_sub(OLD_VERSION_THRESHOLD) {
									// Version is so old we know it's irrelevant; skip waiter registration
									let _ = wait_tx.send(());
								} else {
									waiters.entry(mark.version).or_default().push(wait_tx);
								}
							} else {
								process_one(mark.version, mark.done, &mut pending, &mut waiters, &mut indices);
							}
						}
						None => {
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
