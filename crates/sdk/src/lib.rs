// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Surface that external code uses to extend ReifyDB - building operators, procedures, transforms, flows, and custom
//! connectors that plug into the engine. The crate exposes the FFI boundary that out-of-process extensions implement,
//! the marshalling for values that cross that boundary, and a testing harness so an extension author can run their
//! code against an in-process engine without a server.
//!
//! The shapes of catalog objects, RQL fragments, and result rows that an extension sees here are stable contracts:
//! the SDK is what insulates third-party code from internal refactors of the engine, the planner, or the storage tier.
//! Anything that crosses this boundary - identifiers, errors, columnar payloads - has a versioned representation, and
//! widening that representation requires a coordinated bump on both sides.
//!
//! Invariant: the FFI layer does not leak engine-internal types; everything an extension sees comes either from
//! `reifydb-type` or from this crate's own re-exports. Reaching for an internal engine type from inside an SDK module
//! ties extension ABI to engine refactors.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod catalog;
pub mod connector;
pub mod error;
pub mod ffi;
pub mod flow;
pub mod marshal;
pub mod operator;
pub mod procedure;
pub mod rql;
pub mod state;
pub mod store;
pub mod testing;
pub mod transform;
