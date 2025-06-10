// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::view::deferred::{Engine, Work};
use reifydb_core::AsyncCowVec;
use reifydb_flow::{CountNode, Graph, GroupNode, Orchestrator, SumNode};
use reifydb_storage::Storage;
use std::sync::mpsc::Receiver;

impl<S: Storage + 'static> Engine<S> {
    pub(crate) fn worker(&self, rx: Receiver<Work>) {
        let mut orchestrator = Orchestrator::default();

        orchestrator.register("view::count", create_count_graph(self.storage.clone()));
        orchestrator.register("view::sum", create_sum_graph(self.storage.clone()));

        for (deltas, version) in rx {
            println!("[worker] processing version {:?}, delta count: {}", version, deltas.len());

            orchestrator.apply("view::count", deltas.clone(), version);
            orchestrator.apply("view::sum", deltas, version);

            for stored in
                self.storage.scan_prefix(&AsyncCowVec::new(b"view::count".to_vec()), 2).into_iter()
            {
                println!("{:?}", String::from_utf8(stored.key.to_vec()));
                println!("{:?}", stored.bytes.to_vec().as_slice());
            }

            for sv in
                self.storage.scan_prefix(&AsyncCowVec::new(b"view::sum".to_vec()), 2).into_iter()
            {
                println!("{:?}", String::from_utf8(sv.key.to_vec()));
                println!("{:?}", sv.bytes.to_vec().as_slice());
            }
        }
    }
}

fn create_count_graph<S: Storage + 'static>(storage: S) -> Graph {
    let group_node =
        Box::new(GroupNode { state_prefix: b"view::group_count".to_vec(), group_by: vec![0, 1] });

    let count_node = Box::new(CountNode { state_prefix: b"view::count".to_vec(), storage });
    let mut result = Graph::new(group_node);
    result.add_node(count_node);
    result.connect(0, 1);
    result
}

fn create_sum_graph<S: Storage + 'static>(storage: S) -> Graph {
    let group_node =
        Box::new(GroupNode { state_prefix: b"view::group_count".to_vec(), group_by: vec![0, 1] });
    let count_node = Box::new(SumNode { state_prefix: b"view::sum".to_vec(), storage, sum: 2 });
    let mut result = Graph::new(group_node);
    result.add_node(count_node);
    result.connect(0, 1);
    result
}
