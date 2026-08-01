// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Test-only companion to `reifydb-sdk`: the in-process operator harness, the change/row
//! builders, and the FFI-operator chaos framework.
//!
//! Separate from `reifydb-sdk` because that crate is a mandatory dependency of the umbrella
//! crate, so anything inside it - however gated - every production build has to resolve.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod builders;
pub mod callbacks;
pub mod chaos;
pub mod context;
pub mod harness;
pub mod helpers;
pub mod registry;
pub mod state;
