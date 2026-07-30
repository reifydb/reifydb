// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Test-only companion to `reifydb-sdk`: the in-process operator harness, the change/row builders an
//! extension author writes fixtures with, and the FFI-operator chaos framework layered on the shared
//! substrate in `reifydb-testing-chaos`.
//!
//! It is a separate crate rather than a feature of `reifydb-sdk` because the SDK is a mandatory
//! dependency of the umbrella crate: anything living inside it, however well gated, is a dependency
//! every production build has to resolve. Here the harness is reachable only from a dev-dependency
//! or from `reifydb::testing::sdk`.

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
