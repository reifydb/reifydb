// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! C ABI definitions for the FFI boundary that out-of-process operators, procedures, transforms, flows, and
//! connectors implement. Defines the `repr(C)` shapes of catalog handles, columnar data, callbacks, contexts, and
//! constants that cross the host-extension boundary in either direction.
//!
//! The crate has no logic - only types and constants. Both sides of the boundary depend on it: the host side wraps
//! these types in safe Rust through `reifydb-sdk`; the guest side links against them directly to expose its symbols
//! to the host.
//!
//! Invariant: every type here is wire-stable. Adding, removing, reordering, or resizing a field is a coordinated
//! breaking change for every distributed extension that links against an older version of this crate. New fields go
//! at the end of structs and behind a versioned context handle that the host inspects before reading them.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod callbacks;
pub mod catalog;
pub mod connector;
pub mod constants;
pub mod context;
pub mod data;
pub mod flow;
pub mod operator;
pub mod procedure;
pub mod transform;
