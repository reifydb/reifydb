// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Read-only, structurally-typed projections over columns.
//!
//! The `group_by` view groups column rows by key without copying the underlying data, so aggregations can iterate in
//! group order without materialising an intermediate sorted column.

pub mod group_by;
