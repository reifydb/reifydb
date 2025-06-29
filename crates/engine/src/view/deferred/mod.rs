// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod work;

use crate::view::flow::Orchestrator;
use crate::view::deferred::work::work;
use reifydb_core::delta::Delta;
use reifydb_core::hook::PostCommitHook;
use reifydb_core::{AsyncCowVec, Version};
use reifydb_core::interface::{UnversionedStorage, VersionedStorage};
use std::sync::mpsc::Sender;
use std::sync::{Arc, mpsc};
use std::thread;

pub struct Engine<VS: VersionedStorage, US: UnversionedStorage> {
    tx: Sender<Work>,
    _orchestrator: Orchestrator,
    _marker: std::marker::PhantomData<(VS, US)>,
}

pub(crate) type Work = (AsyncCowVec<Delta>, Version);

impl<VS: VersionedStorage + 'static, US: UnversionedStorage + 'static> Engine<VS, US> {
    pub fn new(storage: VS) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();

        let mut orchestrator = Orchestrator::default();
        orchestrator.register("view::count", work::create_count_graph(storage.clone()));
        orchestrator.register("view::sum", work::create_sum_graph(storage.clone()));

        let result = Arc::new(Engine {
            tx,
            _marker: std::marker::PhantomData,
            _orchestrator: orchestrator.clone(),
        });

        thread::spawn(move || {
            work(rx, storage, orchestrator);
        });

        result
    }
}

impl<VS: VersionedStorage + 'static, US: UnversionedStorage + 'static> PostCommitHook
    for Engine<VS, US>
{
    fn on_post_commit(&self, deltas: AsyncCowVec<Delta>, version: Version) {
        let _ = self.tx.send((deltas, version));
    }
}
