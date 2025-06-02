// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Version;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;

pub trait LogicalClock {
    fn next(&self) -> Version;
    fn current(&self) -> Version;
    fn reset(&self, version: Version);
}

/// A node-local clock
#[derive(Debug, Default)]
pub struct LocalClock {
    sequence: AtomicU64,
}

impl LocalClock {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogicalClock for LocalClock {
    fn next(&self) -> Version {
        self.sequence.fetch_add(1, SeqCst)
    }

    fn current(&self) -> Version {
        self.sequence.load(SeqCst)
    }

    fn reset(&self, version: Version) {
        self.sequence.store(version, SeqCst);
    }
}
