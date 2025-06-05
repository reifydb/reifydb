// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::watermark::Closer;
use crate::mvcc::watermark::watermark::WatermarkInner;
use crossbeam_channel::{Sender, select};
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::Ordering;

impl WatermarkInner {
    pub(crate) fn process(&self, closer: Closer) {
        let mut indices: BinaryHeap<Reverse<u64>> = BinaryHeap::new();
        let pending: RefCell<HashMap<u64, i64>> = RefCell::new(HashMap::new());
        let waiters: RefCell<HashMap<u64, Vec<Sender<()>>>> = RefCell::new(HashMap::new());

        let mut process_one = |idx: u64, done: bool| {
            // If not already done, then set. Otherwise, don't undo a done entry.
            let mut pending = pending.borrow_mut();
            let mut waiters = waiters.borrow_mut();

            if !pending.contains_key(&idx) {
                indices.push(Reverse(idx));
            }

            let mut delta = 1;
            if done {
                delta = -1;
            }
            pending.entry(idx).and_modify(|v| *v += delta).or_insert(delta);

            // Update mark by going through all indices in order; and checking if they have
            // been done. Stop at the first index, which isn't done.
            let done_until = self.done_until.load(Ordering::SeqCst);
            assert!(
                done_until <= idx,
                "name: {}, done_until: {}, idx: {}",
                self.name,
                done_until,
                idx
            );

            let mut until = done_until;

            while !indices.is_empty() {
                let min = indices.peek().unwrap().0;
                if let Some(done) = pending.get(&min) {
                    if done.gt(&0) {
                        break; // len(indices) will be > 0.
                    }
                }
                // Even if done is called multiple times causing it to become
                // negative, we should still pop the index.
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

            if until - done_until <= waiters.len() as u64 {
                // Close channel and remove from waiters.
                (done_until + 1..=until).for_each(|idx| {
                    let _ = waiters.remove(&idx);
                });
            } else {
                // Close and drop idx <= util channels.
                waiters.retain(|idx, _| *idx > until);
            }
        };

        let receiver = closer.listen();
        loop {
            select! {
              recv(receiver) -> _ => { closer.done(); return },
              recv(self.rx) -> mark => match mark {
                Ok(mark) => {
                  if let Some(wait_tx) = mark.waiter {
                      let done_until = self.done_until.load(Ordering::SeqCst);
                      if done_until >= mark.version {
                        let _ = wait_tx; // Close channel.
                      } else if mark.version + 100 < done_until {
                         // Version is so old we know it’s irrelevant; skip waiter registration
                         let _ = wait_tx;
                      } else {
                        waiters.borrow_mut().entry(mark.version).or_default().push(wait_tx);
                      }
                  } else {
                      process_one(mark.version, mark.done)
                  }
                },
                Err(_) => {
                  // Channel closed.
                  println!("watermark has been dropped.");
                  closer.done();
                  return;
                }
              },
            }
        }
    }
}
