// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::catalog::{flow::FlowNodeId, shape::ShapeId};

pub mod accumulator;
pub mod statement;
pub mod storage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricsId {
	Shape(ShapeId),

	FlowNode(FlowNodeId),

	System,
}
