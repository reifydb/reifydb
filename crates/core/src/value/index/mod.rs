// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Index data structures used by the engine during planning and execution.
//!
//! Covers encoded entry layout, point-get and range-get lookups, set-based membership, and shape-aware indexing. These
//! structures complement the on-disk indexes; they exist to accelerate join and filter selectivity decisions inside a
//! single query rather than to persist across queries.

pub mod encoded;
pub mod get;
pub mod range;
pub mod set;
pub mod shape;
