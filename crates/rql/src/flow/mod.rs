// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
pub mod persist;
pub mod plan;
