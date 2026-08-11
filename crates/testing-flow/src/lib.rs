// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Test-only companion to `reifydb-sub-flow`, kept out of that crate because every build that
//! runs flows resolves it.
//!
//! Unlike the sdk harness, which erases the group state a test names, [`harness::Harness`] drives
//! operators against a real `FlowTransaction` and operator state store, so the state a test observes is the state
//! production would hold.

#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod bridge;
pub mod generator;
pub mod harness;
pub mod state;

pub use bridge::assert_backend_parity;

/// Named here so a guest author never has to import `ApplyOperator` from `reifydb-sub-flow` just
/// to write down the type of a fixture.
pub type GuestHarness = harness::Harness<reifydb_flow::operator::apply::ApplyOperator>;
