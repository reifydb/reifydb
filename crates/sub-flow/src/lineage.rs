// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::flow::FlowId;
use reifydb_engine::vm::flow_lineage::ViewLineage;
use reifydb_rql::flow::{analyzer::FlowGraphAnalyzer, flow::FlowDag};
use reifydb_runtime::sync::mutex::Mutex;

#[derive(Clone)]
pub struct FlowLineageTracker {
	inner: Arc<Mutex<FlowGraphAnalyzer>>,
	handle: ViewLineage,
}

impl FlowLineageTracker {
	pub fn new(handle: ViewLineage) -> Self {
		Self {
			inner: Arc::new(Mutex::new(FlowGraphAnalyzer::new())),
			handle,
		}
	}

	pub fn add(&self, flow: FlowDag) {
		let mut analyzer = self.inner.lock();
		analyzer.add(flow);
		self.handle.publish(analyzer.get_dependency_graph().upstream_closure());
	}

	pub fn remove(&self, flow_id: FlowId) {
		let mut analyzer = self.inner.lock();
		analyzer.remove(flow_id);
		self.handle.publish(analyzer.get_dependency_graph().upstream_closure());
	}

	pub fn clear(&self) {
		let mut analyzer = self.inner.lock();
		analyzer.clear();
		self.handle.publish(Default::default());
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeSet;

	use reifydb_core::interface::catalog::{
		flow::{FlowId, FlowNodeId},
		id::{TableId, ViewId},
		shape::ShapeId,
	};
	use reifydb_rql::flow::node::{FlowNode, FlowNodeType};

	use super::*;

	fn flow(id: u64, node_types: Vec<FlowNodeType>) -> FlowDag {
		let mut builder = FlowDag::builder(FlowId(id));
		for (i, ty) in node_types.into_iter().enumerate() {
			builder.add_node(FlowNode::new(FlowNodeId(i as u64 + 1), ty));
		}
		builder.build()
	}

	#[test]
	fn test_mixed_kind_chain_publishes_combined_closure() {
		let handle = ViewLineage::default();
		let tracker = FlowLineageTracker::new(handle.clone());

		// table 100 -> view 200 (would be deferred) -> view 300 (would be
		// transactional): the tracker holds both flows, so the closure
		// walks straight through the boundary.
		tracker.add(flow(
			1,
			vec![
				FlowNodeType::SourceTable {
					table: TableId(100),
				},
				FlowNodeType::SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		));
		tracker.add(flow(
			2,
			vec![
				FlowNodeType::SourceView {
					view: ViewId(200),
				},
				FlowNodeType::SinkTableView {
					view: ViewId(300),
					table: TableId(0),
				},
			],
		));

		assert_eq!(
			*handle.upstream_of(ViewId(300)).unwrap(),
			BTreeSet::from([ShapeId::Table(TableId(100)), ShapeId::View(ViewId(200))]),
			"the combined closure must cross view-kind boundaries"
		);
	}

	#[test]
	fn test_add_is_idempotent_by_flow_id() {
		let handle = ViewLineage::default();
		let tracker = FlowLineageTracker::new(handle.clone());

		let dag = flow(
			1,
			vec![
				FlowNodeType::SourceTable {
					table: TableId(100),
				},
				FlowNodeType::SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		);
		tracker.add(dag.clone());
		tracker.add(dag);

		assert_eq!(*handle.upstream_of(ViewId(200)).unwrap(), BTreeSet::from([ShapeId::Table(TableId(100))]));
	}

	#[test]
	fn test_remove_and_clear_publish() {
		let handle = ViewLineage::default();
		let tracker = FlowLineageTracker::new(handle.clone());

		tracker.add(flow(
			1,
			vec![
				FlowNodeType::SourceTable {
					table: TableId(100),
				},
				FlowNodeType::SinkTableView {
					view: ViewId(200),
					table: TableId(0),
				},
			],
		));
		tracker.remove(FlowId(1));
		assert!(handle.upstream_of(ViewId(200)).is_none(), "remove must republish without the flow");

		tracker.add(flow(
			2,
			vec![
				FlowNodeType::SourceTable {
					table: TableId(100),
				},
				FlowNodeType::SinkTableView {
					view: ViewId(201),
					table: TableId(0),
				},
			],
		));
		tracker.clear();
		assert!(handle.upstream_of(ViewId(201)).is_none(), "clear must publish an empty snapshot");
	}
}
