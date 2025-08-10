// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::watermark::Closer;
use crossbeam_channel::{Receiver, Sender, bounded, RecvTimeoutError};
use reifydb_core::Version;
use std::ops::Deref;
use std::time::Duration;
use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Debug)]
pub struct WatermarkInner {
    pub(crate) done_until: AtomicU64,
    pub(crate) last_index: AtomicU64,
    #[allow(dead_code)]  // Used in debug messages
    pub(crate) name: Cow<'static, str>,
    pub(crate) tx: Sender<Mark>,
    pub(crate) rx: Receiver<Mark>,
}

#[derive(Debug)]
pub(crate) struct Mark {
    pub(crate) version: Version,
    pub(crate) waiter: Option<Sender<()>>,
    pub(crate) done: bool,
}

/// WaterMark is used to keep track of the minimum un-finished index. Typically, an index k becomes
/// finished or "done" according to a WaterMark once `done(k)` has been called
///  1. as many times as `begin(k)` has, AND
///  2. a positive number of times.
#[derive(Debug)]
pub struct WaterMark(Arc<WatermarkInner>);

impl Deref for WaterMark {
    type Target = WatermarkInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WaterMark {
    /// Create a new WaterMark with given name and closer.
    pub fn new(name: Cow<'static, str>, closer: Closer) -> Self {
        let (tx, rx) = bounded(100);

        let inner = Arc::new(WatermarkInner {
            done_until: AtomicU64::new(0),
            last_index: AtomicU64::new(0),
            name,
            tx,
            rx,
        });

        let processing_inner = inner.clone();
        std::thread::spawn(move || {
            processing_inner.process(closer);
        });

        Self(inner)
    }

    /// Sets the last index to the given value.
    pub fn begin(&self, version: Version) {
        // Use fetch_max to handle concurrent calls properly
        let prev = self.last_index.fetch_max(version, Ordering::SeqCst);
        
        // Only send if this is actually a new maximum or equal
        if version >= prev {
            // Handle channel error gracefully
            if self.tx.send(Mark { version, waiter: None, done: false }).is_err() {
                // Channel closed, watermark is shutting down
                return;
            }
        }
    }

    /// Sets a single index as done.
    pub fn done(&self, index: u64) {
        // Handle channel error gracefully
        let _ = self.tx.send(Mark { version: index, waiter: None, done: true });
    }

    /// Returns the maximum index that has the property that all indices
    /// less than or equal to it are done.
    pub fn done_until(&self) -> u64 {
        self.done_until.load(Ordering::SeqCst)
    }

    /// Waits until the given index is marked as done with a default timeout.
    pub fn wait_for_mark(&self, index: u64) {
        self.wait_for_mark_timeout(index, Duration::from_secs(30));
    }
    
    /// Waits until the given index is marked as done with a specified timeout.
    pub fn wait_for_mark_timeout(&self, index: u64, timeout: Duration) -> bool {
        if self.done_until.load(Ordering::SeqCst) >= index {
            return true;
        }

        let (wait_tx, wait_rx) = bounded(1);
        
        // Handle send error
        if self.tx.send(Mark { version: index, waiter: Some(wait_tx), done: false }).is_err() {
            // Channel closed
            return false;
        }

        // Add timeout to prevent indefinite blocking
        match wait_rx.recv_timeout(timeout) {
            Ok(_) => true,
            Err(RecvTimeoutError::Timeout) => {
                // Timeout occurred
                false
            }
            Err(RecvTimeoutError::Disconnected) => {
                // Channel closed
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        init_and_close(|_| {});
    }

    #[test]
    fn test_begin_done() {
        init_and_close(|watermark| {
            watermark.begin(1);
            watermark.begin(2);
            watermark.begin(3);

            watermark.done(1);
            watermark.done(2);
            watermark.done(3);
        });
    }

    #[test]
    fn test_wait_for_mark() {
        init_and_close(|watermark| {
            watermark.begin(1);
            watermark.begin(2);
            watermark.begin(3);

            watermark.done(2);
            watermark.done(3);

            assert_eq!(watermark.done_until(), 0);

            watermark.done(1);
            watermark.wait_for_mark(1);
            watermark.wait_for_mark(3);
            assert_eq!(watermark.done_until(), 3);
        });
    }

    #[test]
    fn test_done_until() {
        init_and_close(|watermark| {
            watermark.done_until.store(1, Ordering::SeqCst);
            assert_eq!(watermark.done_until(), 1);
        });
    }

    fn init_and_close<F>(f: F)
    where
        F: FnOnce(&WaterMark),
    {
        let closer = Closer::new(1);

        let watermark = WaterMark::new("watermark".into(), closer.clone());
        assert_eq!(watermark.name, "watermark");

        f(&watermark);

        closer.signal_and_wait();
    }
}
