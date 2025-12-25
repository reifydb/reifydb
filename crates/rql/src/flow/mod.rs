// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Flow module for ReifyDB RQL
//!
//! This module provides the flow graph types and utilities for representing
//! streaming dataflow computations. The actual compilation from physical plans
//! to flows has been moved to reifydb-engine to avoid lifetime issues with
//! async recursion and generic MultiVersionCommandTransaction types.

pub mod analyzer;
pub mod conversion;
pub mod flow;
pub mod graph;
pub mod loader;
pub mod node;

// Re-export the flow types for external use
pub use self::{
	analyzer::{
		FlowDependency, FlowDependencyGraph, FlowGraphAnalyzer, FlowSummary, PrimitiveReference, SinkReference,
	},
	flow::{Flow, FlowBuilder},
	loader::load_flow,
	node::{FlowEdge, FlowNode, FlowNodeType},
};
