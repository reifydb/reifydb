// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

//! FFI-safe data marshalling types for row and column data

mod buffer;
mod column;
mod layout;

pub use buffer::*;
pub use column::*;
pub use layout::*;
