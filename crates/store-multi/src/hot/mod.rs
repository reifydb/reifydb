// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod memory;
pub mod result;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

pub mod storage;
