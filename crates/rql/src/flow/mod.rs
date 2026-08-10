// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! RQL planning for CREATE FLOW, producing the persisted flow definition `sub-flow` runs. Dataflow shape - which
//! operator depends on which - is settled here at plan time, never in the streaming runtime.

pub mod aggregate;
pub mod analyzer;
pub mod compiler;
#[allow(clippy::module_inception)]
pub mod flow;
pub mod graph;
pub mod loader;
pub mod operator;
pub mod persist;
pub mod plan;
pub mod time_domain;
