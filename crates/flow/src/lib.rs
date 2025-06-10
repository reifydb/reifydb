// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
use reifydb_core::delta::Delta;
use reifydb_core::delta::Delta::Set;
use reifydb_core::encoding::keycode::serialize;
use reifydb_core::{AsyncCowVec, Key, Row, Value, Version, deserialize_row, serialize_row};
use reifydb_storage::Storage;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

mod error;

pub type NodeId = usize;

pub trait Node {
    fn apply(&mut self, delta: &Vec<Delta>, version: Version) -> Vec<Delta>;
}

pub struct Graph {
    pub nodes: HashMap<NodeId, Box<dyn Node>>,
    pub edges: HashMap<NodeId, Vec<NodeId>>,
    pub next_id: NodeId,
}

impl Graph {
    pub fn new(root: Box<dyn Node>) -> Self {
        let mut result = Self { nodes: HashMap::new(), edges: HashMap::new(), next_id: 0 };
        result.add_node(root);
        result
    }

    pub fn add_node(&mut self, node: Box<dyn Node>) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        self.nodes.insert(id, node);
        id
    }

    pub fn root_node() -> NodeId {
        0
    }

    pub fn connect(&mut self, from: NodeId, to: NodeId) {
        self.edges.entry(from).or_default().push(to);
    }

    pub fn apply(&mut self, delta: Vec<Delta>, version: Version) {
        let mut queue = VecDeque::new();
        queue.push_back((Self::root_node(), delta));

        while let Some((node_id, delta)) = queue.pop_front() {
            let node = self.nodes.get_mut(&node_id).expect("invalid node id");

            let output = node.apply(&delta, version);

            for &downstream in self.edges.get(&node_id).unwrap_or(&vec![]) {
                queue.push_back((downstream, output.clone()));
            }
        }
    }
}

pub struct Orchestrator {
    graphs: HashMap<&'static str, Graph>,
    dependencies: HashMap<&'static str, Vec<&'static str>>,
}

impl Orchestrator {
    pub fn register(&mut self, name: &'static str, graph: Graph) {
        self.graphs.insert(name, graph);
    }

    pub fn add_dependency(&mut self, parent: &'static str, child: &'static str) {
        self.dependencies.entry(parent).or_default().push(child);
    }

    pub fn apply(&mut self, root: &'static str, delta: Vec<Delta>, version: Version) {
        let mut queue = VecDeque::new();
        queue.push_back((root, delta));

        while let Some((view_name, input)) = queue.pop_front() {
            let graph = self.graphs.get_mut(view_name).unwrap();
            graph.apply(input.clone(), version);

            // if let Some(children) = self.dependencies.get(view_name) {
            //     for &child in children {
            //         // Pull the materialized delta from `view_name`'s keyspace
            //         let materialized = read_as_deltas(child); // you define how!
            //         queue.push_back((child, materialized));
            //     }
            // }
        }
    }
}

pub struct CountNode<S: Storage> {
    pub state_prefix: Vec<u8>,
    pub storage: Arc<S>,
}

impl<S: Storage> CountNode<S> {
    fn make_state_key(&self, key: &Key) -> Key {
        let mut raw = self.state_prefix.clone();
        raw.extend_from_slice(b"::");
        raw.extend_from_slice(key.as_slice());
        AsyncCowVec::new(raw)
    }
}

impl<S: Storage> Node for CountNode<S> {
    fn apply(&mut self, delta: &Vec<Delta>, version: Version) -> Vec<Delta> {
        let mut updates = Vec::new();
        let mut counters: HashMap<Key, i8> = HashMap::new();

        for d in delta {
            if let Delta::Set { key, .. } = d {
                let state_key = self.make_state_key(key);

                let current = *counters.entry(state_key.clone()).or_insert_with(|| {
                    self.storage.get(&state_key, version).map(|v| v.bytes[0] as i8).unwrap_or(0)
                });

                counters.insert(state_key, current.saturating_add(1));
            }
        }

        for (key, count) in counters {
            updates.push(Set { key, bytes: AsyncCowVec::new(vec![count as u8]) });
        }

        self.storage.apply(updates.clone(), version);

        updates
    }
}

pub struct GroupNode {
    pub state_prefix: Vec<u8>,
    pub group_by: Vec<usize>, // column indexes
}

impl GroupNode {
    // pub fn new(group_by: Vec<usize>) -> Self {
    //     Self { group_by }
    // }

    fn make_group_key(&self, row: &[Value]) -> Key {
        // let values: Row = deserialize_row(&row).unwrap();
        let mut raw = self.state_prefix.clone();
        for &index in &self.group_by {
            raw.extend_from_slice(b"::".as_slice());
            raw.extend(serialize(&row[index].to_string()));
        }
        AsyncCowVec::new(raw)
    }
}

impl Node for GroupNode {
    fn apply(&mut self, delta: &Vec<Delta>, _version: Version) -> Vec<Delta> {
        let mut grouped: HashMap<Key, Vec<Vec<Value>>> = HashMap::new();

        for d in delta {
            if let Delta::Set { bytes, .. } = d {
                let row: Row = deserialize_row(bytes).unwrap();
                let group_key = self.make_group_key(&row);
                grouped.entry(group_key).or_default().push(row);
            }
        }

        grouped
            .into_iter()
            .flat_map(|(key, rows)| {
                rows.into_iter().map(move |r| Delta::Set {
                    key: key.clone(),
                    bytes: AsyncCowVec::new(serialize_row(&r).unwrap()),
                })
            })
            .collect()
    }
}

pub struct SumNode<S: Storage> {
    pub state_prefix: Vec<u8>,
    pub storage: Arc<S>,
    pub sum: usize, // Index of the column to sum
}

impl<S: Storage> SumNode<S> {
    fn make_state_key(&self, key: &Key) -> Key {
        let mut raw = self.state_prefix.clone();
        raw.extend_from_slice(b"::");
        raw.extend_from_slice(key.as_slice());
        AsyncCowVec::new(raw)
    }
}

impl<S: Storage> Node for SumNode<S> {
    fn apply(&mut self, delta: &Vec<Delta>, version: Version) -> Vec<Delta> {
        let mut updates = Vec::new();
        let mut sums: HashMap<Key, i8> = HashMap::new();

        for d in delta {
            if let Delta::Set { key, bytes } = d {
                let state_key = self.make_state_key(key);

                let current = *sums.entry(state_key.clone()).or_insert_with(|| {
                    self.storage.get(&state_key, version).map(|v| v.bytes[0] as i8).unwrap_or(0)
                });

                let values: Row = deserialize_row(bytes).unwrap();

                match &values[self.sum] {
                    Value::Int1(v) => {
                        sums.insert(state_key, current.saturating_add(*v));
                    }
                    _ => unimplemented!("only Value::Int1 is supported for SUM"),
                }
            }
        }

        for (key, sum) in sums {
            updates.push(Set {
                key,
                bytes: AsyncCowVec::new(vec![sum as u8]), // Upgrade to i64 if needed
            });
        }

        self.storage.apply(updates.clone(), version);

        updates
    }
}

#[cfg(test)]
mod tests {
    use crate::{CountNode, Graph, GroupNode, Orchestrator, SumNode};
    use reifydb_core::delta::Delta;
    use reifydb_core::{AsyncCowVec, Value, serialize_row};
    use reifydb_storage::memory::Memory;
    use reifydb_storage::{ScanRange, Storage};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_count_graph<S: Storage + 'static>(storage: Arc<S>) -> Graph {
        let group_node = Box::new(GroupNode {
            state_prefix: b"view::group_count".to_vec(),
            group_by: vec![0, 1],
        });

        let count_node = Box::new(CountNode { state_prefix: b"view::count".to_vec(), storage });
        let mut result = Graph::new(group_node);
        result.add_node(count_node);
        result.connect(0, 1);
        result
    }

    fn create_sum_graph<S: Storage + 'static>(storage: Arc<S>) -> Graph {
        let group_node = Box::new(GroupNode {
            state_prefix: b"view::group_count".to_vec(),
            group_by: vec![0, 1],
        });
        let count_node = Box::new(SumNode { state_prefix: b"view::sum".to_vec(), storage, sum: 2 });
        let mut result = Graph::new(group_node);
        result.add_node(count_node);
        result.connect(0, 1);
        result
    }

    #[test]
    fn test() {
        let mut orchestrator =
            Orchestrator { graphs: HashMap::new(), dependencies: HashMap::new() };

        let storage = Arc::new(Memory::default());
        // let storage = Arc::new(Sqlite::new(Path::new("test.sqlite")));

        orchestrator.register("view::count", create_count_graph(storage.clone()));
        orchestrator.register("view::sum", create_sum_graph(storage.clone()));

        let delta = vec![
            Delta::Set {
                key: AsyncCowVec::new(b"apple".to_vec()),
                bytes: AsyncCowVec::new(
                    serialize_row(&vec![Value::Int1(1), Value::Int1(1), Value::Int1(23)]).unwrap(),
                ),
            },
            Delta::Set {
                key: AsyncCowVec::new(b"apple".to_vec()),
                bytes: AsyncCowVec::new(
                    serialize_row(&vec![Value::Int1(1), Value::Int1(1), Value::Int1(1)]).unwrap(),
                ),
            },
            Delta::Set {
                key: AsyncCowVec::new(b"banana".to_vec()),
                bytes: AsyncCowVec::new(
                    serialize_row(&vec![Value::Int1(2), Value::Int1(1), Value::Int1(1)]).unwrap(),
                ),
            },
            // Delta::Remove { key: AsyncCowVec::new(b"apple".to_vec()) },
        ];

        orchestrator.apply("view::count", delta.clone(), 1);
        orchestrator.apply("view::sum", delta.clone(), 1);

        for sv in storage.scan_prefix(&AsyncCowVec::new(b"view::count".to_vec()), 2).into_iter() {
            println!("{:?}", String::from_utf8(sv.key.to_vec()));
            println!("{:?}", sv.bytes.to_vec().as_slice());
        }

        for sv in storage.scan_prefix(&AsyncCowVec::new(b"view::sum".to_vec()), 2).into_iter() {
            println!("{:?}", String::from_utf8(sv.key.to_vec()));
            println!("{:?}", sv.bytes.to_vec().as_slice());
        }
    }
}
