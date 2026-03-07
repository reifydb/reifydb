// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Marshaling implementations for FFI types
//!
//! Marshal/unmarshal methods are implemented directly on Arena.

pub mod column;
pub mod types;
pub mod util;
pub mod wasm;

pub(crate) mod flow;
