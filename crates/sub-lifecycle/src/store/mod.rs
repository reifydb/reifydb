// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Executors for the store-owned lifecycle components. The `FlushEngine` and `DropEngine` themselves stay in
//! `reifydb-store-multi` because the store calls them on its commit path; these tasks drive them on a schedule.

pub mod drop;
pub mod flush;
pub mod tombstone;
pub mod vacuum;
