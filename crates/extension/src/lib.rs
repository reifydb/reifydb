// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

//! Unified extension loading for ReifyDB (FFI, WASM, native)

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod error;
pub mod function;
pub mod loader;
pub mod operator;
pub mod procedure;
pub mod transform;
