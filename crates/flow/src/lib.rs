// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
use reifydb_core::AsyncCowVec;
use reifydb_storage::Delta::Set;
use reifydb_storage::{Delta, Key, Storage, Version};
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
        let mut new_key = self.state_prefix.clone();
        new_key.extend_from_slice(key);
        AsyncCowVec::new(new_key)
    }
}

impl<S: Storage> Node for CountNode<S> {
    fn apply(&mut self, delta: &Vec<Delta>, version: Version) -> Vec<Delta> {
        let mut updates = Vec::new();
        let mut counters: HashMap<Key, i8> = HashMap::new();

        for d in delta {
            match d {
                Delta::Set { key, value } => {
                    let state_key = self.make_state_key(key);

                    let current = *counters.entry(state_key.clone()).or_insert_with(|| {
                        self.storage.get(&state_key, version).map(|v| v.value[0] as i8).unwrap_or(0)
                    });

                    counters.insert(state_key, current.saturating_add(1));
                }
                Delta::Remove { key } => {
                    let state_key = self.make_state_key(key);
                    counters.remove(&state_key);
                }
            }
        }

        for (key, count) in &counters {
            let out = Set { key: key.clone(), value: AsyncCowVec::new(vec![*count as u8]) };
            updates.push(out);
        }

        self.storage.apply(updates.clone(), version);

        updates
    }
}
#[cfg(test)]
mod tests {
    use crate::{CountNode, Graph, Orchestrator};
    use reifydb_core::AsyncCowVec;
    use reifydb_storage::memory::Memory;
    use reifydb_storage::{Delta, Scan, Storage};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_count_graph<S: Storage + 'static>(storage: Arc<S>) -> Graph {
        let count_node = Box::new(CountNode { state_prefix: b"view::count::".to_vec(), storage });
        Graph::new(count_node)
    }

    #[test]
    fn test() {
        let mut orchestrator =
            Orchestrator { graphs: HashMap::new(), dependencies: HashMap::new() };

        let storage = Arc::new(Memory::default());
        // let storage = Arc::new(Sqlite::new(Path::new("test.sqlite")));
        let graph = create_count_graph(storage.clone());

        orchestrator.register("view::count", graph);

        let delta = vec![
            Delta::Set {
                key: AsyncCowVec::new(b"apple".to_vec()),
                value: AsyncCowVec::new(vec![1]),
            },
            Delta::Set {
                key: AsyncCowVec::new(b"apple".to_vec()),
                value: AsyncCowVec::new(vec![1]),
            },
            Delta::Set {
                key: AsyncCowVec::new(b"banana".to_vec()),
                value: AsyncCowVec::new(vec![1]),
            },
            // Delta::Remove { key: AsyncCowVec::new(b"apple".to_vec()) },
        ];

        orchestrator.apply("view::count", delta.clone(), 1);
        orchestrator.apply("view::count", delta.clone(), 2);

        for sv in storage.scan(2).into_iter() {
            println!("{:?}", String::from_utf8(sv.key.to_vec()));
            println!("{:?}", sv.value.to_vec().as_slice());
        }
    }
}
