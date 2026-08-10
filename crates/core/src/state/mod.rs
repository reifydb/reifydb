// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator state: the [`store`] contract every operator reads and writes through, the [`cache`] facade that
//! forwards to it, and the group/horizon/keyspace vocabulary reclamation works in. Not window-specific: ffi,
//! native, distinct and take all route their state through the same contract.

pub mod cache;
pub mod group;
pub mod horizon;
pub mod store;
