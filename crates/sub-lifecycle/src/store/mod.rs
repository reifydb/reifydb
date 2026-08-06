// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Executors for the store-owned lifecycle components. The `FlushEngine` itself stays in
//! `reifydb-store-multi` because the store calls it on its commit path; these tasks drive it on a schedule.

pub mod flush;
pub mod tombstone;
pub mod vacuum;
