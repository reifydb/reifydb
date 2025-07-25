// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod work;

use crate::view::deferred::work::work;
use crate::view::flow::Orchestrator;
use reifydb_core::delta::Delta;
use reifydb_core::hook::transaction::PostCommitHook;
use reifydb_core::hook::{BoxedHookIter, Callback};
use reifydb_core::interface::{UnversionedStorage, VersionedStorage};
use reifydb_core::{CowVec, Error, Version, return_hooks};
use std::sync::mpsc::Sender;
use std::sync::{Arc, mpsc};
use std::thread;

pub struct Engine<VS: VersionedStorage, US: UnversionedStorage> {
    tx: Sender<Work>,
    _orchestrator: Orchestrator,
    _phantom: std::marker::PhantomData<(VS, US)>,
}

pub(crate) type Work = (CowVec<Delta>, Version);

impl<VS: VersionedStorage, US: UnversionedStorage> Engine<VS, US> {
    pub fn new(storage: VS) -> Arc<Self> {
        let (tx, rx) = mpsc::channel();

        let mut orchestrator = Orchestrator::default();
        orchestrator.register("view::count", work::create_count_graph(storage.clone()));
        orchestrator.register("view::sum", work::create_sum_graph(storage.clone()));

        let result = Arc::new(Engine {
            tx,
            _phantom: std::marker::PhantomData,
            _orchestrator: orchestrator.clone(),
        });

        thread::spawn(move || {
            work(rx, storage, orchestrator);
        });

        result
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Callback<PostCommitHook> for Engine<VS, US> {
    fn on(&self, hook: &PostCommitHook) -> Result<BoxedHookIter, Error> {
        let _ = self.tx.send((hook.deltas.clone(), hook.version));
        return_hooks!()
    }
}
