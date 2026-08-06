// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Test-only companion to `reifydb-sub-flow`, kept out of that crate because every build that
//! runs flows resolves it.
//!
//! Unlike the sdk harness, which erases the group state a test names, [`harness::Harness`] drives
//! production's own tick compaction (`compact_operator`) against a real `FlowTransaction` and
//! arena, so the operator's own floors decide what is dropped; an operator declaring retention it
//! never receives fails here and passes there.

#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod generator;
pub mod harness;
pub mod native;

pub use native::assert_backend_parity;

/// Named here so a guest author never has to import `ApplyOperator` from `reifydb-sub-flow` just
/// to write down the type of a fixture.
pub type GuestHarness = harness::Harness<reifydb_sub_flow::operator::apply::ApplyOperator>;
