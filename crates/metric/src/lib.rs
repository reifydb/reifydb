// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Observability primitives: counters, gauges, histograms, plus the per-statement accumulator the engine uses to
//! attribute work to a query. The crate owns the metric registry, the in-memory storage of recent samples, and the
//! snapshot type external collectors read from. Metrics are addressed by `MetricId` so a single hierarchy can hold
//! shape-scoped, flow-node-scoped, and system-scoped values without colliding.
//!
//! This crate produces the data; `sub-metric` is the subsystem that delivers it to an external sink. Anything that
//! emits a metric inside the engine writes here; downstream readers either iterate the snapshot directly (for
//! in-process use) or subscribe through `sub-metric` (for export).

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricId {
	Shape(ShapeId),

	FlowNode(FlowNodeId),

	System,
}
