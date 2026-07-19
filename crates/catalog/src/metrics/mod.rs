// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{flow::FlowNodeId, shape::ShapeId};

pub mod storage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricsId {
	Shape(ShapeId),

	FlowNode(FlowNodeId),

	System,
}
