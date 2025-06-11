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
use reifydb_core::row::{EncodedRow, deprecated_deserialize_row, deprecated_serialize_row};
use reifydb_core::{AsyncCowVec, EncodedKey, Version};
use reifydb_storage::Storage;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

mod error;

pub type NodeId = usize;

pub trait Node: Send + Sync {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version) -> AsyncCowVec<Delta>;
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

    pub fn apply(&self, delta: AsyncCowVec<Delta>, version: Version) {
        let mut queue = VecDeque::new();
        queue.push_back((Self::root_node(), delta));

        while let Some((node_id, delta)) = queue.pop_front() {
            let node = self.nodes.get(&node_id).expect("invalid node id");

            let output = node.apply(delta.clone(), version);

            for &downstream in self.edges.get(&node_id).unwrap_or(&vec![]) {
                queue.push_back((downstream, output.clone()));
            }
        }
    }
}

#[derive(Clone)]
pub struct Orchestrator(Arc<RwLock<OrchestratorInner>>);

pub struct OrchestratorInner {
    graphs: HashMap<String, Graph>,
    dependencies: HashMap<String, Vec<String>>,
}

impl Orchestrator {
    pub fn register(&mut self, name: impl Into<String>, graph: Graph) {
        let mut guard = self.0.write().unwrap();
        guard.graphs.insert(name.into(), graph);
    }

    pub fn add_dependency(&mut self, parent: impl Into<String>, child: impl Into<String>) {
        let mut guard = self.0.write().unwrap();
        guard.dependencies.entry(parent.into()).or_default().push(child.into());
    }

    pub fn apply(&self, root: &'static str, delta: AsyncCowVec<Delta>, version: Version) {
        let guard = self.0.read().unwrap();
        guard.apply(root, delta, version);
    }
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self(Arc::new(RwLock::new(OrchestratorInner {
            graphs: Default::default(),
            dependencies: Default::default(),
        })))
    }
}

impl OrchestratorInner {
    pub fn apply(&self, root: &'static str, delta: AsyncCowVec<Delta>, version: Version) {
        let mut queue = VecDeque::new();
        queue.push_back((root, delta));

        while let Some((view_name, input)) = queue.pop_front() {
            let graph = self.graphs.get(view_name).unwrap();
            graph.apply(input, version);

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
    pub storage: S,
}

impl<S: Storage> CountNode<S> {
    fn make_state_key(&self, key: &EncodedKey) -> EncodedKey {
        let mut raw = self.state_prefix.clone();
        raw.extend_from_slice(b"::");
        raw.extend_from_slice(key.as_slice());
        EncodedKey::new(raw)
    }
}

impl<S: Storage> Node for CountNode<S> {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version) -> AsyncCowVec<Delta> {
        let mut updates = AsyncCowVec::default();
        let mut counters: HashMap<EncodedKey, i8> = HashMap::new();

        for d in delta {
            if let Delta::Set { key, .. } = d {
                let state_key = self.make_state_key(&key);

                let current = *counters.entry(state_key.clone()).or_insert_with(|| {
                    self.storage.get(&state_key, version).map(|v| v.row[0] as i8).unwrap_or(0)
                });

                counters.insert(state_key, current.saturating_add(1));
            }
        }

        for (key, count) in counters {
            updates.push(Set { key, row: EncodedRow(AsyncCowVec::new(vec![count as u8])) });
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

    fn make_group_key(&self, row: &EncodedRow) -> EncodedKey {
        // let values: Row = deserialize_row(&row).unwrap();
        let mut raw = self.state_prefix.clone();
        for &index in &self.group_by {
            raw.extend_from_slice(b"::".as_slice());
            raw.extend(serialize(&row[index].to_string()));
        }
        EncodedKey::new(raw)
    }
}

impl Node for GroupNode {
    fn apply(&self, delta: AsyncCowVec<Delta>, _version: Version) -> AsyncCowVec<Delta> {
        let mut grouped: HashMap<EncodedKey, Vec<EncodedRow>> = HashMap::new();

        for d in delta {
            if let Delta::Set { row, .. } = d {
                let row: EncodedRow = deprecated_deserialize_row(&row).unwrap();
                let group_key = self.make_group_key(&row);
                grouped.entry(group_key).or_default().push(row);
            }
        }

        AsyncCowVec::new(
            grouped
                .into_iter()
                .flat_map(|(key, rows)| {
                    rows.into_iter().map(move |r| Delta::Set {
                        key: key.clone(),
                        row: EncodedRow(AsyncCowVec::new(deprecated_serialize_row(&r).unwrap())),
                    })
                })
                .collect(),
        )
    }
}

pub struct SumNode<S: Storage> {
    pub state_prefix: Vec<u8>,
    pub storage: S,
    pub sum: usize, // Index of the column to sum
}

impl<S: Storage> SumNode<S> {
    fn make_state_key(&self, key: &EncodedKey) -> EncodedKey {
        let mut raw = self.state_prefix.clone();
        raw.extend_from_slice(b"::");
        raw.extend_from_slice(key.as_slice());
        EncodedKey::new(raw)
    }
}

impl<S: Storage> Node for SumNode<S> {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version) -> AsyncCowVec<Delta> {
        let mut updates = AsyncCowVec::default();
        let mut sums: HashMap<EncodedKey, i8> = HashMap::new();

        for d in delta {
            if let Delta::Set { key, row } = d {
                let state_key = self.make_state_key(&key);

                let current = *sums.entry(state_key.clone()).or_insert_with(|| {
                    self.storage.get(&state_key, version).map(|v| v.row[0] as i8).unwrap_or(0)
                });

                // let row: Row = deserialize_row(&bytes).unwrap();
                //
                // match &row[self.sum] {
                //     Value::Int1(v) => {
                //         sums.insert(state_key, current.saturating_add(*v));
                //     }
                //     _ => unimplemented!("only Value::Int1 is supported for SUM"),
                // }
                // unimplemented!()
            }
        }

        for (key, sum) in sums {
            updates.push(Set { key, row: EncodedRow(AsyncCowVec::new(vec![sum as u8])) });
        }

        self.storage.apply(updates.clone(), version);
        updates
    }
}

#[cfg(test)]
mod tests {
    use crate::{CountNode, Graph, GroupNode, SumNode};
    use reifydb_storage::Storage;

    fn create_count_graph<S: Storage + 'static>(storage: S) -> Graph {
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

    fn create_sum_graph<S: Storage + 'static>(storage: S) -> Graph {
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

    // #[test]
    // fn test() {
    //     let mut orchestrator = Orchestrator::default();
    //
    //     let storage = Memory::default();
    //     // let storage = Arc::new(Sqlite::new(Path::new("test.sqlite")));
    //
    //     orchestrator.register("view::count", create_count_graph(storage.clone()));
    //     orchestrator.register("view::sum", create_sum_graph(storage.clone()));
    //
    //     // let delta = AsyncCowVec::new(vec![
    //     //     Delta::Set {
    //     //         key: EncodedKey::new(b"apple".to_vec()),
    //     //         bytes: AsyncCowVec::new(
    //     //             serialize_row(&vec![Value::Int1(1), Value::Int1(1), Value::Int1(23)]).unwrap(),
    //     //         ),
    //     //     },
    //     //     Delta::Set {
    //     //         key: EncodedKey::new(b"apple".to_vec()),
    //     //         bytes: AsyncCowVec::new(
    //     //             serialize_row(&vec![Value::Int1(1), Value::Int1(1), Value::Int1(1)]).unwrap(),
    //     //         ),
    //     //     },
    //     //     Delta::Set {
    //     //         key: EncodedKey::new(b"banana".to_vec()),
    //     //         bytes: AsyncCowVec::new(
    //     //             serialize_row(&vec![Value::Int1(2), Value::Int1(1), Value::Int1(1)]).unwrap(),
    //     //         ),
    //     //     },
    //     //     // Delta::Remove { key: EncodedKey::new(b"apple".to_vec()) },
    //     // ]);
    //     //
    //     // orchestrator.apply("view::count", delta.clone(), 1);
    //     // orchestrator.apply("view::sum", delta.clone(), 1);
    //     //
    //     // for sv in storage.scan_prefix(&AsyncCowVec::new(b"view::count".to_vec()), 2).into_iter() {
    //     //     println!("{:?}", String::from_utf8(sv.key.to_vec()));
    //     //     println!("{:?}", sv.bytes.to_vec().as_slice());
    //     // }
    //     //
    //     // for sv in storage.scan_prefix(&AsyncCowVec::new(b"view::sum".to_vec()), 2).into_iter() {
    //     //     println!("{:?}", String::from_utf8(sv.key.to_vec()));
    //     //     println!("{:?}", sv.bytes.to_vec().as_slice());
    //     // }
    //     unimplemented!()
    // }
}
