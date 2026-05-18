// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RQL planning for CREATE FLOW. Analyses the source query, builds the operator graph, plans each operator
//! against the catalog, and produces the persisted flow definition the engine hands to `sub-flow` at runtime.
//! Dataflow shape - which operator depends on which - is settled here, not in the streaming runtime.

pub mod analyzer;
#[allow(clippy::module_inception)]
pub mod flow;
pub mod graph;
pub mod loader;
pub mod node;
pub mod persist;
pub mod plan;
