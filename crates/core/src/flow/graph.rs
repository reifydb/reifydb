use std::collections::{HashMap, HashSet, VecDeque};
use crate::flow::node::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Edge {
    pub source: NodeId,
    pub target: NodeId,
}

#[derive(Debug, Clone)]
pub struct DirectedGraph<NodeData> {
    nodes: HashMap<NodeId, NodeData>,
    edges: Vec<Edge>,
    outgoing: HashMap<NodeId, Vec<NodeId>>,
    incoming: HashMap<NodeId, Vec<NodeId>>,
}

impl<NodeData> DirectedGraph<NodeData> {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node_id: NodeId, data: NodeData) -> NodeId {
        self.nodes.insert(node_id.clone(), data);
        self.outgoing.entry(node_id.clone()).or_insert_with(Vec::new);
        self.incoming.entry(node_id.clone()).or_insert_with(Vec::new);
        node_id
    }

    pub fn add_edge(&mut self, source: &NodeId, target: &NodeId) {
        if !self.nodes.contains_key(source) {
            panic!("Source node {:?} does not exist", source);
        }
        if !self.nodes.contains_key(target) {
            panic!("Target node {:?} does not exist", target);
        }

        // Check for cycles before adding edge
        if self.would_create_cycle(source, target) {
            panic!("Adding edge would create a cycle");
        }

        let edge = Edge { source: source.clone(), target: target.clone() };

        self.edges.push(edge);

        self.outgoing.entry(source.clone()).or_insert_with(Vec::new).push(target.clone());
        self.incoming.entry(target.clone()).or_insert_with(Vec::new).push(source.clone());
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<&NodeData> {
        self.nodes.get(node_id)
    }

    pub fn get_node_mut(&mut self, node_id: &NodeId) -> Option<&mut NodeData> {
        self.nodes.get_mut(node_id)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn neighbors(&self, node_id: &NodeId) -> Vec<NodeId> {
        self.outgoing.get(node_id).cloned().unwrap_or_default()
    }

    pub fn predecessors(&self, node_id: &NodeId) -> Vec<NodeId> {
        self.incoming.get(node_id).cloned().unwrap_or_default()
    }

    pub fn topological_sort(&self) -> Vec<NodeId> {
        let mut in_degree = HashMap::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        // Calculate in-degrees
        for node_id in self.nodes.keys() {
            in_degree.insert(node_id.clone(), 0);
        }

        for edge in &self.edges {
            *in_degree.get_mut(&edge.target).unwrap() += 1;
        }

        // Add nodes with no incoming edges to queue
        for (node_id, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(node_id.clone());
            }
        }

        // Process nodes
        while let Some(node_id) = queue.pop_front() {
            result.push(node_id.clone());

            // Update in-degrees of neighbors
            if let Some(neighbors) = self.outgoing.get(&node_id) {
                for neighbor in neighbors {
                    let degree = in_degree.get_mut(neighbor).unwrap();
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(neighbor.clone());
                    }
                }
            }
        }

        if result.len() != self.nodes.len() {
            panic!("Graph contains cycles");
        }

        result
    }

    pub fn dfs_from(&self, start: &NodeId) -> Vec<NodeId> {
        let mut visited = HashSet::new();
        let mut result = Vec::new();
        let mut stack = vec![start.clone()];

        while let Some(node_id) = stack.pop() {
            if visited.insert(node_id.clone()) {
                result.push(node_id.clone());

                if let Some(neighbors) = self.outgoing.get(&node_id) {
                    for neighbor in neighbors.iter().rev() {
                        if !visited.contains(neighbor) {
                            stack.push(neighbor.clone());
                        }
                    }
                }
            }
        }

        result
    }

    pub fn bfs_from(&self, start: &NodeId) -> Vec<NodeId> {
        let mut visited = HashSet::new();
        let mut result = Vec::new();
        let mut queue = VecDeque::new();

        queue.push_back(start.clone());
        visited.insert(start.clone());

        while let Some(node_id) = queue.pop_front() {
            result.push(node_id.clone());

            if let Some(neighbors) = self.outgoing.get(&node_id) {
                for neighbor in neighbors {
                    if visited.insert(neighbor.clone()) {
                        queue.push_back(neighbor.clone());
                    }
                }
            }
        }

        result
    }

    fn would_create_cycle(&self, source: &NodeId, target: &NodeId) -> bool {
        // Check if adding edge from source to target would create a cycle
        // This happens if there's already a path from target to source
        let reachable = self.dfs_from(target);
        reachable.contains(source)
    }

    pub fn nodes(&self) -> impl Iterator<Item = (&NodeId, &NodeData)> {
        self.nodes.iter()
    }

    pub fn edges(&self) -> impl Iterator<Item = &Edge> {
        self.edges.iter()
    }

    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<NodeData> {
        if let Some(data) = self.nodes.remove(node_id) {
            // Remove all edges involving this node
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

    pub fn edges_directed(&self, node_id: &NodeId, direction: EdgeDirection) -> Vec<&Edge> {
        match direction {
            EdgeDirection::Incoming => {
                self.edges.iter().filter(|edge| edge.target == *node_id).collect()
            }
            EdgeDirection::Outgoing => {
                self.edges.iter().filter(|edge| edge.source == *node_id).collect()
            }
        }
    }

    pub fn edge_indices(&self) -> std::ops::Range<usize> {
        0..self.edges.len()
    }

    pub fn edge_endpoints(&self, edge_index: usize) -> Option<(&NodeId, &NodeId)> {
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
mod tests {
    use super::*;

    #[test]
    fn test_basic_graph_operations() {
        let mut graph = DirectedGraph::new();

        let node1 = graph.add_node(NodeId(1), "Node 1");
        let node2 = graph.add_node(NodeId(2), "Node 2");
        let node3 = graph.add_node(NodeId(3), "Node 3");

        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 0);

        graph.add_edge(&node1, &node2);
        graph.add_edge(&node2, &node3);

        assert_eq!(graph.edge_count(), 2);
        assert_eq!(graph.neighbors(&node1), vec![NodeId(2)]);
        assert_eq!(graph.neighbors(&node2), vec![NodeId(3)]);
        assert_eq!(graph.predecessors(&node3), vec![NodeId(2)]);
    }

    #[test]
    #[should_panic(expected = "Adding edge would create a cycle")]
    fn test_cycle_detection() {
        let mut graph = DirectedGraph::new();

        let node1 = graph.add_node(NodeId(1), "Node 1");
        let node2 = graph.add_node(NodeId(2), "Node 2");
        let node3 = graph.add_node(NodeId(3), "Node 3");

        graph.add_edge(&node1, &node2);
        graph.add_edge(&node2, &node3);

        // This should create a cycle and panic
        graph.add_edge(&node3, &node1);
    }

    #[test]
    fn test_topological_sort() {
        let mut graph = DirectedGraph::new();

        let node1 = graph.add_node(NodeId(1), "Node 1");
        let node2 = graph.add_node(NodeId(2), "Node 2");
        let node3 = graph.add_node(NodeId(3), "Node 3");

        graph.add_edge(&node1, &node2);
        graph.add_edge(&node2, &node3);

        let sorted = graph.topological_sort();
        assert_eq!(sorted, vec![NodeId(1), NodeId(2), NodeId(3)]);
    }

    #[test]
    fn test_dfs_traversal() {
        let mut graph = DirectedGraph::new();

        let node1 = graph.add_node(NodeId(1), "Node 1");
        let node2 = graph.add_node(NodeId(2), "Node 2");
        let node3 = graph.add_node(NodeId(3), "Node 3");
        let node4 = graph.add_node(NodeId(4), "Node 4");

        graph.add_edge(&node1, &node2);
        graph.add_edge(&node1, &node3);
        graph.add_edge(&node2, &node4);

        let dfs_result = graph.dfs_from(&node1);
        assert!(dfs_result.contains(&NodeId(1)));
        assert!(dfs_result.contains(&NodeId(2)));
        assert!(dfs_result.contains(&NodeId(3)));
        assert!(dfs_result.contains(&NodeId(4)));
        assert_eq!(dfs_result.len(), 4);
    }
}
