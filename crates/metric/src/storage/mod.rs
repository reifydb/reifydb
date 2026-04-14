// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Storage and CDC metric persistence.
//!
//! Duplicated from `reifydb-metric-old` during the actor-based migration.
//! `metric-old` still owns the `MetricsWorker` + `MetricReader` facade while this
//! module provides the same writers/readers for the new actor-based path.

pub mod cdc;
pub mod encoding;
pub mod multi;
pub mod parser;
