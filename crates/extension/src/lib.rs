// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Host side of the extension contract: resolves and binds extension symbols for the engine, whether the extension
//! is a native dynamic library, a WASM module, or an in-tree Rust function. Extension authors target `reifydb-sdk`
//! instead of depending on this crate.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod callbacks;
pub mod error;
pub mod function;
pub mod loader;
pub mod operator;
pub mod procedure;
pub mod transform;
