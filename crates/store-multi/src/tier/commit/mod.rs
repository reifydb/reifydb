// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod buffer;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod domain;
pub mod memory;
pub mod result;
