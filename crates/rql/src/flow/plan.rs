// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compiled flow plan types for 2-phase flow compilation.
//!
//! These types represent the output of Phase 1 (compilation) before
//! catalog IDs are assigned in Phase 2 (persistence).

use reifydb_core::interface::ViewId;

use super::node::FlowNodeType;

/// Local node ID used during compilation (before catalog IDs are assigned).
///
/// These are simple incrementing integers starting from 0, assigned during
/// the compilation phase. They are mapped to real `FlowNodeId` values
/// during the persistence phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalNodeId(pub u32);

/// A compiled node with a local ID (not yet persisted to catalog).
#[derive(Debug, Clone)]
pub struct CompiledNode {
	/// Local ID assigned during compilation
	pub local_id: LocalNodeId,
	/// The node type with all data needed for execution
	pub node_type: FlowNodeType,
}

/// A compiled edge connecting two nodes (using local IDs).
#[derive(Debug, Clone)]
pub struct CompiledEdge {
	/// Local source node ID
	pub source: LocalNodeId,
	/// Local target node ID
	pub target: LocalNodeId,
}

/// The output of Phase 1: a complete flow plan ready for persistence.
///
/// This is a pure data structure with no catalog references. It contains
/// all the information needed to persist the flow in Phase 2.
#[derive(Debug, Clone)]
pub struct CompiledFlowPlan {
	/// Nodes in topological order (sources first, sinks last)
	pub nodes: Vec<CompiledNode>,
	/// Edges connecting nodes
	pub edges: Vec<CompiledEdge>,
	/// Optional sink view for terminal output
	pub sink_view: Option<ViewId>,
}
