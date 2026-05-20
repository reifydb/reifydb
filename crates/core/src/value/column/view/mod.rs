// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Read-only, structurally-typed projections over columns.
//!
//! The `group_by` view groups column rows by key without copying the underlying data, so aggregations can iterate in
//! group order without materialising an intermediate sorted column.

pub mod group_by;
