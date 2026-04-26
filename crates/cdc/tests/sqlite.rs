// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! SQLite-backend specific integration tests. The shared trait-level
//! assertions live in `storage.rs`; this binary only carries scenarios that
//! cannot be expressed against `MemoryCdcStorage`.

#[path = "sqlite/compression.rs"]
mod compression;
#[path = "sqlite/dispatch.rs"]
mod dispatch;
#[path = "sqlite/persistence.rs"]
mod persistence;
