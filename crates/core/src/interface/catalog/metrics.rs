// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::{flow::FlowNodeId, shape::ShapeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricsId {
	Shape(ShapeId),

	FlowNode(FlowNodeId),

	System,
}
