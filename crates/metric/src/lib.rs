// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::catalog::{flow::FlowNodeId, shape::ShapeId};

pub mod accumulator;
pub mod buckets;
pub mod counter;
pub mod gauge;
pub mod histogram;
pub mod registry;
pub mod snapshot;
pub mod statement;
pub mod storage;

/// Identifier for tracking per-object storage statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricId {
	/// Table, view, or flow shape
	Shape(ShapeId),
	/// Flow operator node
	FlowNode(FlowNodeId),
	/// System metadata (sequences, versions, etc.)
	System,
}
