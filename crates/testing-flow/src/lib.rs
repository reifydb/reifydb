// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Test-only companion to `reifydb-sub-flow`: the operator harness that drives the subsystem's own
//! reclamation sweep.
//!
//! It is a separate crate rather than a feature of `reifydb-sub-flow` for the same reason
//! `reifydb-testing-sdk` is separate from `reifydb-sdk`: the subsystem is resolved by every build
//! that runs flows, and nothing test-only should ride along with it. Reach it through
//! `reifydb::testing::flow`.
//!
//! What distinguishes [`harness::Harness`] from the guest harness in `reifydb-testing-sdk` is who
//! decides. The sdk harness erases the group state a test names; this one hands production's own
//! `reclaim_nodes` a real `FlowTransaction` over a real engine and lets the sweep decide what is
//! due, so an operator that declares retention it never receives fails here and passes there.

#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod generator;
pub mod harness;
pub mod native;

pub use native::assert_backend_parity;

/// The harness a guest operator is driven through.
///
/// Named here so a consumer never has to reach into `reifydb-sub-flow` for `ApplyOperator` just to
/// write down the type of its own fixture. The wrapper is an implementation detail of how the host
/// hosts a guest, not something a guest author should have to import.
pub type GuestHarness = harness::Harness<reifydb_sub_flow::operator::apply::ApplyOperator>;
