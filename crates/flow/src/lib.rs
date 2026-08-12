// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution substrate in two tiers: the lean default carries only what a guest cdylib can name without
//! linking the host, and `runtime` adds `FlowTransaction` plus the `HostOperator` contract. Lean is the default so a
//! forgotten feature fails the host build loudly rather than linking the catalog, transactions and store into a guest.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod factory;
pub mod state;
pub mod timer;
pub mod window;

#[cfg(feature = "runtime")]
pub mod context;
#[cfg(feature = "runtime")]
pub mod engine;
#[cfg(feature = "runtime")]
pub mod error;
#[cfg(feature = "runtime")]
pub mod operator;
#[cfg(feature = "runtime")]
pub mod transaction;

#[cfg(all(test, feature = "runtime"))]
pub(crate) mod testing;
