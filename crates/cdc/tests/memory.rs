// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Memory-backend specific integration tests. The shared trait-level
//! assertions live in `storage.rs`; this binary only carries scenarios that
//! cannot be expressed against `SqliteCdcStorage`.

#[path = "memory/clone.rs"]
mod clone;
#[path = "memory/concurrency.rs"]
mod concurrency;
