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
use crossbeam_channel::{bounded, Receiver, Sender};
use reifydb_core::Version;
use std::ops::Deref;
use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Debug)]
pub struct WatermarkInner {
    pub(crate) done_until: AtomicU64,
    pub(crate) last_index: AtomicU64,
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
        self.last_index.store(version, Ordering::SeqCst);
        self.tx.send(Mark { version, waiter: None, done: false }).unwrap()
    }

    /// Sets a single index as done.
    pub fn done(&self, index: u64) {
        self.tx.send(Mark { version: index, waiter: None, done: true }).unwrap() // unwrap is safe because self also holds a receiver
    }

    /// Returns the maximum index that has the property that all indices
    /// less than or equal to it are done.
    pub fn done_until(&self) -> u64 {
        self.done_until.load(Ordering::SeqCst)
    }

    /// Waits until the given index is marked as done.
    pub fn wait_for_mark(&self, index: u64) {
        if self.done_until.load(Ordering::SeqCst) >= index {
            return;
        }

        let (wait_tx, wait_rx) = bounded(1);
        self.tx.send(Mark { version: index, waiter: Some(wait_tx), done: false }).unwrap(); // unwrap is safe because self also holds a receiver

        let _ = wait_rx.recv();
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
