// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the wg project (https://github.com/al8n/wg),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0


use std::sync::Arc;
use std::sync::{Condvar, Mutex};

struct Inner {
    cvar: Condvar,
    count: Mutex<usize>,
}

pub struct WaitGroup {
    inner: Arc<Inner>,
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl From<usize> for WaitGroup {
    fn from(count: usize) -> Self {
        Self { inner: Arc::new(Inner { cvar: Condvar::new(), count: Mutex::new(count) }) }
    }
}

impl Clone for WaitGroup {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl std::fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.inner.count.lock().unwrap();
        f.debug_struct("WaitGroup").field("count", &*count).finish()
    }
}

impl WaitGroup {
    pub fn new() -> Self {
        Self { inner: Arc::new(Inner { cvar: Condvar::new(), count: Mutex::new(0) }) }
    }

    pub fn add(&self, num: usize) -> Self {
        let mut ctr = self.inner.count.lock().unwrap();
        *ctr += num;
        Self { inner: self.inner.clone() }
    }

    pub fn done(&self) -> usize {
        let mut val = self.inner.count.lock().unwrap();

        *val = if val.eq(&1) {
            self.inner.cvar.notify_all();
            0
        } else if val.eq(&0) {
            0
        } else {
            *val - 1
        };
        *val
    }

    pub fn waitings(&self) -> usize {
        *self.inner.count.lock().unwrap()
    }

    pub fn wait(&self) {
        let mut ctr = self.inner.count.lock().unwrap();

        if ctr.eq(&0) {
            return;
        }

        while *ctr > 0 {
            ctr = self.inner.cvar.wait(ctr).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::wait_group::WaitGroup;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn test_sync_wait_group_reuse() {
        let wg = WaitGroup::new();
        let ctr = Arc::new(AtomicUsize::new(0));
        for _ in 0..6 {
            let wg = wg.add(1);
            let ctrx = ctr.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5));
                ctrx.fetch_add(1, Ordering::Relaxed);
                wg.done();
            });
        }

        wg.wait();
        assert_eq!(ctr.load(Ordering::Relaxed), 6);

        let worker = wg.add(1);
        let ctrx = ctr.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            ctrx.fetch_add(1, Ordering::Relaxed);
            worker.done();
        });
        wg.wait();
        assert_eq!(ctr.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn test_sync_wait_group_nested() {
        let wg = WaitGroup::new();
        let ctr = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            let worker = wg.add(1);
            let ctrx = ctr.clone();
            std::thread::spawn(move || {
                let nested_worker = worker.add(1);
                let ctrxx = ctrx.clone();
                std::thread::spawn(move || {
                    ctrxx.fetch_add(1, Ordering::Relaxed);
                    nested_worker.done();
                });
                ctrx.fetch_add(1, Ordering::Relaxed);
                worker.done();
            });
        }

        wg.wait();
        assert_eq!(ctr.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_sync_wait_group_from() {
        std::thread::scope(|s| {
            let wg = WaitGroup::from(5);
            for _ in 0..5 {
                let t = wg.clone();
                s.spawn(move || {
                    t.done();
                });
            }
            wg.wait();
        });
    }

    #[test]
    fn test_clone_and_fmt() {
        let swg = WaitGroup::new();
        let swg1 = swg.clone();
        swg1.add(3);
        assert_eq!(format!("{:?}", swg), format!("{:?}", swg1));
    }

    #[test]
    fn test_waitings() {
        let wg = WaitGroup::new();
        wg.add(1);
        wg.add(1);
        assert_eq!(wg.waitings(), 2);
    }
}
