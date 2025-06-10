// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod work;

use crate::view::deferred::work::work;
use reifydb_core::delta::Delta;
use reifydb_core::hook::PostCommitHook;
use reifydb_core::{AsyncCowVec, Version};
use reifydb_flow::Orchestrator;
use reifydb_storage::Storage;
use std::sync::mpsc::Sender;
use std::sync::{Arc, mpsc};
use std::thread;

pub struct Engine<S: Storage> {
    tx: Sender<Work>,
    orchestrator: Orchestrator,
    _marker: std::marker::PhantomData<S>,
}

pub(crate) type Work = (AsyncCowVec<Delta>, Version);

impl<S: Storage + 'static> Engine<S> {
    pub fn new(storage: S) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();

        let mut orchestrator = Orchestrator::default();
        orchestrator.register("view::count", work::create_count_graph(storage.clone()));
        orchestrator.register("view::sum", work::create_sum_graph(storage.clone()));

        let result = Arc::new(Engine {
            tx,
            _marker: std::marker::PhantomData,
            orchestrator: orchestrator.clone(),
        });

        thread::spawn(move || {
            work(rx, storage, orchestrator);
        });

        result
    }
}

impl<S: Storage + 'static> PostCommitHook for Engine<S> {
    fn on_post_commit(&self, deltas: AsyncCowVec<Delta>, version: Version) {
        let _ = self.tx.send((deltas, version));
    }
}
