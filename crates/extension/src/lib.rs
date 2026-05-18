// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Unified loader for ReifyDB extensions, regardless of whether the extension is a native dynamic library, a WASM
//! module, or an in-tree Rust function. Wraps the FFI symbol-resolution machinery, registers callbacks the host
//! provides for the guest, and exposes the typed handles (operator, procedure, function, transform) that the engine
//! uses to dispatch into extension code.
//!
//! Extension authors do not depend on this crate directly; they target `reifydb-sdk` instead. This crate is the host
//! side of that contract - the place the engine looks when it needs to find and bind a registered extension symbol.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod error;
pub mod ffi_callbacks;
pub mod function;
pub mod loader;
pub mod operator;
pub mod procedure;
pub mod transform;
