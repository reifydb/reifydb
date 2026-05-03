// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod memory;
pub mod result;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

pub mod storage;
