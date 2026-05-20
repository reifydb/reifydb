// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
