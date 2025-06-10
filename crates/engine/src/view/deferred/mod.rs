// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod work;

use reifydb_core::delta::Delta;
use reifydb_core::hook::PostCommitHook;
use reifydb_core::{AsyncCowVec, Version};
use reifydb_storage::Storage;
use std::sync::mpsc::Sender;
use std::sync::{Arc, mpsc};
use std::thread;

pub struct Engine<S: Storage> {
    tx: Sender<Work>,
    storage: S,
}

pub(crate) type Work = (AsyncCowVec<Delta>, Version);

impl<S: Storage + 'static> Engine<S> {
    pub fn new(storage: S) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();
        let result = Arc::new(Engine { tx, storage });
        let thread_engine = Arc::clone(&result);
        thread::spawn(move || {
            thread_engine.worker(rx);
        });
        result
    }
}

impl<S: Storage + 'static> PostCommitHook for Engine<S> {
    fn on_post_commit(&self, deltas: AsyncCowVec<Delta>, version: Version) {
        let _ = self.tx.send((deltas, version));
    }
}
