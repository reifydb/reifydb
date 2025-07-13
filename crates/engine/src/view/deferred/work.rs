// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::view::deferred::Work;
use crate::view::flow::{CountNode, Graph, GroupNode, Orchestrator, SumNode};
use reifydb_core::interface::VersionedStorage;
use std::sync::mpsc::Receiver;

pub(crate) fn work<VS: VersionedStorage>(
    _rx: Receiver<Work>,
    _storage: VS,
    _orchestrator: Orchestrator,
) {
    // for (deltas, version) in rx {
    // println!("[worker] processing version {:?}, delta count: {}", version, deltas.len());
    //
    // orchestrator.apply("view::count", deltas.clone(), version);
    // orchestrator.apply("view::sum", deltas, version);
    //
    // for stored in storage.scan_prefix(&EncodedKey::new(b"view::count".to_vec()), 2).into_iter()
    // {
    //     println!("{:?}", String::from_utf8(stored.key.to_vec()));
    //     println!("{:?}", stored.row.to_vec().as_slice());
    // }
    //
    // for sv in storage.scan_prefix(&EncodedKey::new(b"view::sum".to_vec()), 2).into_iter() {
    //     println!("{:?}", String::from_utf8(sv.key.to_vec()));
    //     println!("{:?}", sv.row.to_vec().as_slice());
    // }
    // }
}

pub(crate) fn create_count_graph<VS: VersionedStorage>(storage: VS) -> Graph {
    let group_node =
        Box::new(GroupNode { state_prefix: b"view::group_count".to_vec(), group_by: vec![0, 1] });

    let count_node = Box::new(CountNode { state_prefix: b"view::count".to_vec(), storage });
    let mut result = Graph::new(group_node);
    result.add_node(count_node);
    result.connect(0, 1);
    result
}

pub(crate) fn create_sum_graph<VS: VersionedStorage>(storage: VS) -> Graph {
    let group_node =
        Box::new(GroupNode { state_prefix: b"view::group_count".to_vec(), group_by: vec![0, 1] });
    let count_node = Box::new(SumNode { state_prefix: b"view::sum".to_vec(), storage, sum: 2 });
    let mut result = Graph::new(group_node);
    result.add_node(count_node);
    result.connect(0, 1);
    result
}
