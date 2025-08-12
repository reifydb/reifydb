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
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use reifydb_core::Version;
use std::ops::Deref;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;
use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub struct WatermarkInner {
    pub(crate) done_until: AtomicU64,
    pub(crate) last_index: AtomicU64,
    #[allow(dead_code)] // Used in debug messages
    pub(crate) name: Cow<'static, str>,
    pub(crate) tx: Sender<Mark>,
    pub(crate) rx: Receiver<Mark>,
    pub(crate) processor_thread: Mutex<Option<JoinHandle<()>>>,
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
pub struct WaterMark(Arc<WatermarkInner>);

impl std::fmt::Debug for WaterMark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaterMark")
            .field("name", &self.name)
            .field("done_until", &self.done_until.load(Ordering::Relaxed))
            .field("last_index", &self.last_index.load(Ordering::Relaxed))
            .finish()
    }
}

impl Deref for WaterMark {
    type Target = WatermarkInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WaterMark {
    /// Create a new WaterMark with given name and closer.
    pub fn new(name: Cow<'static, str>, closer: Closer) -> Self {
        let (tx, rx) = bounded(super::WATERMARK_CHANNEL_SIZE);

        let inner = Arc::new(WatermarkInner {
            done_until: AtomicU64::new(0),
            last_index: AtomicU64::new(0),
            name,
            tx,
            rx,
            processor_thread: Mutex::new(None),
        });

        let processing_inner = inner.clone();
        let thread_handle = std::thread::spawn(move || {
            processing_inner.process(closer);
        });

        // Store the thread handle
        *inner.processor_thread.lock().unwrap() = Some(thread_handle);

        Self(inner)
    }

    /// Sets the last index to the given value.
    pub fn begin(&self, version: Version) {
        // Update last_index to the maximum
        self.last_index.fetch_max(version, Ordering::SeqCst);

        // Always send the mark - the processing thread will handle ordering
        // Handle channel error gracefully
        let _ = self.tx.send(Mark { version, waiter: None, done: false });
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
    use std::time::Instant;

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

    #[test]
    fn test_high_concurrency() {
        use std::sync::Arc;
        use std::thread;

        let closer = Closer::new(1);
        let watermark = Arc::new(WaterMark::new("concurrent".into(), closer.clone()));

        const NUM_THREADS: usize = 50;
        const OPS_PER_THREAD: usize = 100;

        let mut handles = vec![];

        // Spawn threads that perform concurrent begin/done operations
        for thread_id in 0..NUM_THREADS {
            let wm = watermark.clone();
            let handle = thread::spawn(move || {
                for i in 0..OPS_PER_THREAD {
                    let version = (thread_id * OPS_PER_THREAD + i) as u64 + 1;
                    wm.begin(version);
                    wm.done(version);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        thread::sleep(Duration::from_millis(100));

        // Verify the watermark progressed
        let final_done = watermark.done_until();
        assert!(final_done > 0, "Watermark should have progressed");

        closer.signal_and_wait();
    }

    #[test]
    fn test_concurrent_wait_for_mark() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        let closer = Closer::new(1);
        let watermark = Arc::new(WaterMark::new("wait_concurrent".into(), closer.clone()));
        let success_count = Arc::new(AtomicUsize::new(0));

        // Start some versions
        for i in 1..=10 {
            watermark.begin(i);
        }

        let mut handles = vec![];

        // Spawn threads that wait for marks
        for version in 1..=10 {
            let wm = watermark.clone();
            let counter = success_count.clone();
            let handle = thread::spawn(move || {
                // Use timeout to avoid hanging if something goes wrong
                if wm.wait_for_mark_timeout(version, Duration::from_secs(5)) {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        // Give threads time to start waiting
        thread::sleep(Duration::from_millis(50));

        // Complete the versions
        for i in 1..=10 {
            watermark.done(i);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // All waits should have succeeded
        assert_eq!(success_count.load(Ordering::Relaxed), 10);

        closer.signal_and_wait();
    }

    #[test]
    fn test_old_version_rejection() {
        use std::thread;

        init_and_close(|watermark| {
            // Advance done_until significantly
            for i in 1..=100 {
                watermark.begin(i);
                watermark.done(i);
            }

            // Wait for processing
            thread::sleep(Duration::from_millis(50));

            let done_until = watermark.done_until();
            assert!(done_until >= 50, "Should have processed many versions");

            // Try to wait for a very old version (should return immediately)
            let very_old = done_until.saturating_sub(super::super::OLD_VERSION_THRESHOLD + 10);
            let start = Instant::now();
            watermark.wait_for_mark(very_old);
            let elapsed = start.elapsed();

            // Should return almost immediately (< 10ms)
            assert!(elapsed.as_millis() < 10, "Old version wait should return immediately");
        });
    }

    #[test]
    fn test_timeout_behavior() {
        init_and_close(|watermark| {
            // Begin but don't complete a version
            watermark.begin(1);

            // Wait with short timeout
            let start = Instant::now();
            let result = watermark.wait_for_mark_timeout(1, Duration::from_millis(100));
            let elapsed = start.elapsed();

            // Should timeout and return false
            assert!(!result, "Should timeout waiting for uncompleted version");
            assert!(
                elapsed.as_millis() >= 100 && elapsed.as_millis() < 200,
                "Should respect timeout duration"
            );
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
