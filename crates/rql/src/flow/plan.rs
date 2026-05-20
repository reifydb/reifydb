// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::ViewId;

use super::node::FlowNodeType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalNodeId(pub u32);

#[derive(Debug, Clone)]
pub struct CompiledNode {
	pub local_id: LocalNodeId,

	pub node_type: FlowNodeType,
}

#[derive(Debug, Clone)]
pub struct CompiledEdge {
	pub source: LocalNodeId,

	pub target: LocalNodeId,
}

#[derive(Debug, Clone)]
pub struct CompiledFlowPlan {
	pub nodes: Vec<CompiledNode>,

	pub edges: Vec<CompiledEdge>,

	pub sink_view: Option<ViewId>,
}
