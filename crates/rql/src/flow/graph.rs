// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BTreeMap, BinaryHeap, HashSet, VecDeque},
	ops,
};

use ops::Range;
use reifydb_core::interface::catalog::flow::{FlowEdgeId, FlowNodeId};

use super::node::FlowEdge;

#[derive(Debug, Clone)]
pub struct DirectedGraph<NodeData> {
	nodes: BTreeMap<FlowNodeId, NodeData>,
	edges: Vec<FlowEdge>,
	outgoing: BTreeMap<FlowNodeId, Vec<FlowNodeId>>,
	incoming: BTreeMap<FlowNodeId, Vec<FlowNodeId>>,
}

impl<NodeData> DirectedGraph<NodeData> {
	pub fn new() -> Self {
		Self {
			nodes: BTreeMap::new(),
			edges: Vec::new(),
			outgoing: BTreeMap::new(),
			incoming: BTreeMap::new(),
		}
	}

	pub fn add_node(&mut self, node_id: FlowNodeId, data: NodeData) -> FlowNodeId {
		self.nodes.insert(node_id, data);
		self.outgoing.entry(node_id).or_default();
		self.incoming.entry(node_id).or_default();
		node_id
	}

	pub fn add_edge(&mut self, edge: FlowEdge) -> FlowEdgeId {
		let source = edge.source;
		let target = edge.target;

		let result = edge.id;

		if !self.nodes.contains_key(&source) {
			panic!("Source operator {:?} does not exist", source);
		}
		if !self.nodes.contains_key(&target) {
			panic!("Target operator {:?} does not exist", target);
		}

		// Check for cycles before adding edge
		if self.creates_cycle(&source, &target) {
			panic!("Adding edge would create a cycle");
		}

		self.edges.push(edge);

		self.outgoing.entry(source).or_default().push(target);

		self.incoming.entry(target).or_default().push(source);

		result
	}

	pub fn get_node(&self, node_id: &FlowNodeId) -> Option<&NodeData> {
		self.nodes.get(node_id)
	}

	pub fn get_node_mut(&mut self, node_id: &FlowNodeId) -> Option<&mut NodeData> {
		self.nodes.get_mut(node_id)
	}

	pub fn node_count(&self) -> usize {
		self.nodes.len()
	}

	pub fn edge_count(&self) -> usize {
		self.edges.len()
	}

	pub fn neighbors(&self, node_id: &FlowNodeId) -> Vec<FlowNodeId> {
		self.outgoing.get(node_id).cloned().unwrap_or_default()
	}

	pub fn predecessors(&self, node_id: &FlowNodeId) -> Vec<FlowNodeId> {
		self.incoming.get(node_id).cloned().unwrap_or_default()
	}

	pub fn topological_sort(&self) -> Vec<FlowNodeId> {
		let mut in_degree = BTreeMap::new();
		// Use a min-heap (via Reverse) to ensure deterministic ordering by node ID
		let mut heap: BinaryHeap<Reverse<FlowNodeId>> = BinaryHeap::new();
		let mut result = Vec::new();

		// Calculate in-degrees
		for node_id in self.nodes.keys() {
			in_degree.insert(*node_id, 0);
		}

		for edge in &self.edges {
			*in_degree.get_mut(&edge.target).unwrap() += 1;
		}

		// Add nodes with no incoming edges to heap (sorted by node ID)
		for (node_id, &degree) in &in_degree {
			if degree == 0 {
				heap.push(Reverse(*node_id));
			}
		}

		// Process nodes in deterministic order (smallest node ID first)
		while let Some(Reverse(node_id)) = heap.pop() {
			result.push(node_id);

			// Update in-degrees of neighbors
			if let Some(neighbors) = self.outgoing.get(&node_id) {
				for neighbor in neighbors {
					let degree = in_degree.get_mut(neighbor).unwrap();
					*degree -= 1;
					if *degree == 0 {
						heap.push(Reverse(*neighbor));
					}
				}
			}
		}

		if result.len() != self.nodes.len() {
			panic!("Graph contains cycles");
		}

		result
	}

	pub fn dfs_from(&self, start: &FlowNodeId) -> Vec<FlowNodeId> {
		let mut visited = HashSet::new();
		let mut result = Vec::new();
		let mut stack = vec![*start];

		while let Some(node_id) = stack.pop() {
			if visited.insert(node_id) {
				result.push(node_id);

				if let Some(neighbors) = self.outgoing.get(&node_id) {
					for neighbor in neighbors.iter().rev() {
						if !visited.contains(neighbor) {
							stack.push(*neighbor);
						}
					}
				}
			}
		}

		result
	}

	pub fn bfs_from(&self, start: &FlowNodeId) -> Vec<FlowNodeId> {
		let mut visited = HashSet::new();
		let mut result = Vec::new();
		let mut queue = VecDeque::new();

		queue.push_back(*start);
		visited.insert(*start);

		while let Some(node_id) = queue.pop_front() {
			result.push(node_id);

			if let Some(neighbors) = self.outgoing.get(&node_id) {
				for neighbor in neighbors {
					if visited.insert(*neighbor) {
						queue.push_back(*neighbor);
					}
				}
			}
		}

		result
	}

	fn creates_cycle(&self, source: &FlowNodeId, target: &FlowNodeId) -> bool {
		// Check if adding edge from source to target would create a
		// cycle This happens if there's already a path from target to
		// source
		let reachable = self.dfs_from(target);
		reachable.contains(source)
	}

	pub fn nodes(&self) -> impl Iterator<Item = (&FlowNodeId, &NodeData)> {
		self.nodes.iter()
	}

	pub fn edges(&self) -> impl Iterator<Item = &FlowEdge> {
		self.edges.iter()
	}

	pub fn remove_node(&mut self, node_id: &FlowNodeId) -> Option<NodeData> {
		if let Some(data) = self.nodes.remove(node_id) {
			// Remove all edges involving this operator
			self.edges.retain(|edge| edge.source != *node_id && edge.target != *node_id);

			// Clean up adjacency lists
			self.outgoing.remove(node_id);
			self.incoming.remove(node_id);

			// Remove references from other nodes' adjacency lists
			for (_, outgoing_list) in self.outgoing.iter_mut() {
				outgoing_list.retain(|id| id != node_id);
			}
			for (_, incoming_list) in self.incoming.iter_mut() {
				incoming_list.retain(|id| id != node_id);
			}

			Some(data)
		} else {
			None
		}
	}

	pub fn is_empty(&self) -> bool {
		self.nodes.is_empty()
	}

	pub fn clear(&mut self) {
		self.nodes.clear();
		self.edges.clear();
		self.outgoing.clear();
		self.incoming.clear();
	}

	pub fn edges_directed(&self, node_id: &FlowNodeId, direction: EdgeDirection) -> Vec<&FlowEdge> {
		match direction {
			EdgeDirection::Incoming => self.edges.iter().filter(|edge| edge.target == *node_id).collect(),
			EdgeDirection::Outgoing => self.edges.iter().filter(|edge| edge.source == *node_id).collect(),
		}
	}

	pub fn edge_indices(&self) -> Range<usize> {
		0..self.edges.len()
	}

	pub fn edge_endpoints(&self, edge_index: usize) -> Option<(&FlowNodeId, &FlowNodeId)> {
		self.edges.get(edge_index).map(|edge| (&edge.source, &edge.target))
	}
}

#[derive(Debug, Clone, Copy)]
pub enum EdgeDirection {
	Incoming,
	Outgoing,
}

impl<NodeData> Default for DirectedGraph<NodeData> {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_basic_graph_operations() {
		let mut graph = DirectedGraph::new();

		let node1 = graph.add_node(FlowNodeId(1), "Node 1");
		let node2 = graph.add_node(FlowNodeId(2), "Node 2");
		let node3 = graph.add_node(FlowNodeId(3), "Node 3");

		assert_eq!(graph.node_count(), 3);
		assert_eq!(graph.edge_count(), 0);

		graph.add_edge(FlowEdge::new(1, &node1, &node2));
		graph.add_edge(FlowEdge::new(2, &node2, &node3));

		assert_eq!(graph.edge_count(), 2);
		assert_eq!(graph.neighbors(&node1), vec![FlowNodeId(2)]);
		assert_eq!(graph.neighbors(&node2), vec![FlowNodeId(3)]);
		assert_eq!(graph.predecessors(&node3), vec![FlowNodeId(2)]);
	}

	#[test]
	#[should_panic(expected = "Adding edge would create a cycle")]
	fn test_cycle_detection() {
		let mut graph = DirectedGraph::new();

		let node1 = graph.add_node(FlowNodeId(1), "Node 1");
		let node2 = graph.add_node(FlowNodeId(2), "Node 2");
		let node3 = graph.add_node(FlowNodeId(3), "Node 3");

		graph.add_edge(FlowEdge::new(1, &node1, &node2));
		graph.add_edge(FlowEdge::new(2, &node2, &node3));

		// This should create a cycle and panic
		graph.add_edge(FlowEdge::new(3, &node3, &node1));
	}

	#[test]
	fn test_topological_sort() {
		let mut graph = DirectedGraph::new();

		let node1 = graph.add_node(FlowNodeId(1), "Node 1");
		let node2 = graph.add_node(FlowNodeId(2), "Node 2");
		let node3 = graph.add_node(FlowNodeId(3), "Node 3");

		graph.add_edge(FlowEdge::new(1, &node1, &node2));
		graph.add_edge(FlowEdge::new(2, &node2, &node3));

		let sorted = graph.topological_sort();
		assert_eq!(sorted, vec![FlowNodeId(1), FlowNodeId(2), FlowNodeId(3)]);
	}

	#[test]
	fn test_dfs_traversal() {
		let mut graph = DirectedGraph::new();

		let node1 = graph.add_node(FlowNodeId(1), "Node 1");
		let node2 = graph.add_node(FlowNodeId(2), "Node 2");
		let node3 = graph.add_node(FlowNodeId(3), "Node 3");
		let node4 = graph.add_node(FlowNodeId(4), "Node 4");

		graph.add_edge(FlowEdge::new(1, &node1, &node2));
		graph.add_edge(FlowEdge::new(2, &node1, &node3));
		graph.add_edge(FlowEdge::new(3, &node2, &node4));

		let dfs_result = graph.dfs_from(&node1);
		assert!(dfs_result.contains(&FlowNodeId(1)));
		assert!(dfs_result.contains(&FlowNodeId(2)));
		assert!(dfs_result.contains(&FlowNodeId(3)));
		assert!(dfs_result.contains(&FlowNodeId(4)));
		assert_eq!(dfs_result.len(), 4);
	}
}
